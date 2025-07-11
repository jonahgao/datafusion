// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::datatypes::{DataType, TimeUnit};
use datafusion_expr::planner::{
    PlannerResult, RawBinaryExpr, RawDictionaryExpr, RawFieldAccessExpr,
};
use sqlparser::ast::{
    AccessExpr, BinaryOperator, CastFormat, CastKind, DataType as SQLDataType,
    DictionaryField, Expr as SQLExpr, ExprWithAlias as SQLExprWithAlias,
    FunctionArguments, MapEntry, StructField, Subscript, TrimWhereField, Value,
    ValueWithSpan,
};

use datafusion_common::{
    internal_datafusion_err, internal_err, not_impl_err, plan_err, DFSchema, Result,
    ScalarValue,
};

use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr::{InList, WildcardOptions};
use datafusion_expr::{
    lit, Between, BinaryExpr, Cast, Expr, ExprSchemable, GetFieldAccess, Like, Literal,
    Operator, TryCast,
};

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};

mod binary_op;
mod function;
mod grouping_set;
mod identifier;
mod order_by;
mod subquery;
mod substring;
mod unary_op;
mod value;

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(crate) fn sql_expr_to_logical_expr_with_alias(
        &self,
        sql: SQLExprWithAlias,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let mut expr =
            self.sql_expr_to_logical_expr(sql.expr, schema, planner_context)?;
        if let Some(alias) = sql.alias {
            expr = expr.alias(alias.value);
        }
        Ok(expr)
    }
    pub(crate) fn sql_expr_to_logical_expr(
        &self,
        sql: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        enum StackEntry {
            SQLExpr(Box<SQLExpr>),
            Operator(BinaryOperator),
        }

        // Virtual stack machine to convert SQLExpr to Expr
        // This allows visiting the expr tree in a depth-first manner which
        // produces expressions in postfix notations, i.e. `a + b` => `a b +`.
        // See https://github.com/apache/datafusion/issues/1444
        let mut stack = vec![StackEntry::SQLExpr(Box::new(sql))];
        let mut eval_stack = vec![];

        while let Some(entry) = stack.pop() {
            match entry {
                StackEntry::SQLExpr(sql_expr) => {
                    match *sql_expr {
                        SQLExpr::BinaryOp { left, op, right } => {
                            // Note the order that we push the entries to the stack
                            // is important. We want to visit the left node first.
                            stack.push(StackEntry::Operator(op));
                            stack.push(StackEntry::SQLExpr(right));
                            stack.push(StackEntry::SQLExpr(left));
                        }
                        _ => {
                            let expr = self.sql_expr_to_logical_expr_internal(
                                *sql_expr,
                                schema,
                                planner_context,
                            )?;
                            eval_stack.push(expr);
                        }
                    }
                }
                StackEntry::Operator(op) => {
                    let right = eval_stack.pop().unwrap();
                    let left = eval_stack.pop().unwrap();
                    let expr = self.build_logical_expr(op, left, right, schema)?;
                    eval_stack.push(expr);
                }
            }
        }

        assert_eq!(1, eval_stack.len());
        let expr = eval_stack.pop().unwrap();
        Ok(expr)
    }

    fn build_logical_expr(
        &self,
        op: BinaryOperator,
        left: Expr,
        right: Expr,
        schema: &DFSchema,
    ) -> Result<Expr> {
        // try extension planers
        let mut binary_expr = RawBinaryExpr { op, left, right };
        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_binary_op(binary_expr, schema)? {
                PlannerResult::Planned(expr) => {
                    return Ok(expr);
                }
                PlannerResult::Original(expr) => {
                    binary_expr = expr;
                }
            }
        }

        let RawBinaryExpr { op, left, right } = binary_expr;
        Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            self.parse_sql_binary_op(op)?,
            Box::new(right),
        )))
    }

    pub fn sql_to_expr_with_alias(
        &self,
        sql: SQLExprWithAlias,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let mut expr =
            self.sql_expr_to_logical_expr_with_alias(sql, schema, planner_context)?;
        expr = self.rewrite_partial_qualifier(expr, schema);
        self.validate_schema_satisfies_exprs(schema, &[expr.clone()])?;
        let (expr, _) = expr.infer_placeholder_types(schema)?;
        Ok(expr)
    }

    /// Generate a relational expression from a SQL expression
    pub fn sql_to_expr(
        &self,
        sql: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // The location of the original SQL expression in the source code
        let mut expr = self.sql_expr_to_logical_expr(sql, schema, planner_context)?;
        expr = self.rewrite_partial_qualifier(expr, schema);
        self.validate_schema_satisfies_exprs(schema, std::slice::from_ref(&expr))?;
        let (expr, _) = expr.infer_placeholder_types(schema)?;
        Ok(expr)
    }

    /// Rewrite aliases which are not-complete (e.g. ones that only include only table qualifier in a schema.table qualified relation)
    fn rewrite_partial_qualifier(&self, expr: Expr, schema: &DFSchema) -> Expr {
        match expr {
            Expr::Column(col) => match &col.relation {
                Some(q) => {
                    match schema.iter().find(|(qualifier, field)| match qualifier {
                        Some(field_q) => {
                            field.name() == &col.name
                                && field_q.to_string().ends_with(&format!(".{q}"))
                        }
                        _ => false,
                    }) {
                        Some((qualifier, df_field)) => Expr::from((qualifier, df_field)),
                        None => Expr::Column(col),
                    }
                }
                None => Expr::Column(col),
            },
            _ => expr,
        }
    }

    /// Internal implementation. Use
    /// [`Self::sql_expr_to_logical_expr`] to plan exprs.
    #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
    fn sql_expr_to_logical_expr_internal(
        &self,
        sql: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // NOTE: This function is called recursively, so each match arm body should be as
        //       small as possible to decrease stack requirement.
        //       Follow the common pattern of extracting into a separate function for
        //       non-trivial arms. See https://github.com/apache/datafusion/pull/12384 for
        //       more context.
        match sql {
            SQLExpr::Value(value) => {
                self.parse_value(value.into(), planner_context.prepare_param_data_types())
            }
            SQLExpr::Extract { field, expr, .. } => {
                let mut extract_args = vec![
                    Expr::Literal(ScalarValue::from(format!("{field}")), None),
                    self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
                ];

                for planner in self.context_provider.get_expr_planners() {
                    match planner.plan_extract(extract_args)? {
                        PlannerResult::Planned(expr) => return Ok(expr),
                        PlannerResult::Original(args) => {
                            extract_args = args;
                        }
                    }
                }

                not_impl_err!("Extract not supported by ExprPlanner: {extract_args:?}")
            }

            SQLExpr::Array(arr) => self.sql_array_literal(arr.elem, schema),
            SQLExpr::Interval(interval) => self.sql_interval_to_expr(false, interval),
            SQLExpr::Identifier(id) => {
                self.sql_identifier_to_expr(id, schema, planner_context)
            }

            // <expr>["foo"], <expr>[4] or <expr>[4:5]
            SQLExpr::CompoundFieldAccess { root, access_chain } => self
                .sql_compound_field_access_to_expr(
                    *root,
                    access_chain,
                    schema,
                    planner_context,
                ),

            SQLExpr::CompoundIdentifier(ids) => {
                self.sql_compound_identifier_to_expr(ids, schema, planner_context)
            }

            SQLExpr::Case {
                operand,
                conditions,
                else_result,
            } => self.sql_case_identifier_to_expr(
                operand,
                conditions,
                else_result,
                schema,
                planner_context,
            ),

            SQLExpr::Cast {
                kind: CastKind::Cast | CastKind::DoubleColon,
                expr,
                data_type,
                format,
            } => self.sql_cast_to_expr(*expr, data_type, format, schema, planner_context),

            SQLExpr::Cast {
                kind: CastKind::TryCast | CastKind::SafeCast,
                expr,
                data_type,
                format,
            } => {
                if let Some(format) = format {
                    return not_impl_err!("CAST with format is not supported: {format}");
                }

                Ok(Expr::TryCast(TryCast::new(
                    Box::new(self.sql_expr_to_logical_expr(
                        *expr,
                        schema,
                        planner_context,
                    )?),
                    self.convert_data_type(&data_type)?,
                )))
            }

            SQLExpr::TypedString { data_type, value } => Ok(Expr::Cast(Cast::new(
                Box::new(lit(value.into_string().unwrap())),
                self.convert_data_type(&data_type)?,
            ))),

            SQLExpr::IsNull(expr) => Ok(Expr::IsNull(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::IsDistinctFrom(left, right) => {
                Ok(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(self.sql_expr_to_logical_expr(
                        *left,
                        schema,
                        planner_context,
                    )?),
                    Operator::IsDistinctFrom,
                    Box::new(self.sql_expr_to_logical_expr(
                        *right,
                        schema,
                        planner_context,
                    )?),
                )))
            }

            SQLExpr::IsNotDistinctFrom(left, right) => {
                Ok(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(self.sql_expr_to_logical_expr(
                        *left,
                        schema,
                        planner_context,
                    )?),
                    Operator::IsNotDistinctFrom,
                    Box::new(self.sql_expr_to_logical_expr(
                        *right,
                        schema,
                        planner_context,
                    )?),
                )))
            }

            SQLExpr::IsTrue(expr) => Ok(Expr::IsTrue(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::IsFalse(expr) => Ok(Expr::IsFalse(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::IsNotTrue(expr) => Ok(Expr::IsNotTrue(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::IsNotFalse(expr) => Ok(Expr::IsNotFalse(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::IsUnknown(expr) => Ok(Expr::IsUnknown(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::IsNotUnknown(expr) => Ok(Expr::IsNotUnknown(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::UnaryOp { op, expr } => {
                self.parse_sql_unary_op(op, *expr, schema, planner_context)
            }

            SQLExpr::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(Expr::Between(Between::new(
                Box::new(self.sql_expr_to_logical_expr(
                    *expr,
                    schema,
                    planner_context,
                )?),
                negated,
                Box::new(self.sql_expr_to_logical_expr(*low, schema, planner_context)?),
                Box::new(self.sql_expr_to_logical_expr(
                    *high,
                    schema,
                    planner_context,
                )?),
            ))),

            SQLExpr::InList {
                expr,
                list,
                negated,
            } => self.sql_in_list_to_expr(*expr, list, negated, schema, planner_context),

            SQLExpr::Like {
                negated,
                expr,
                pattern,
                escape_char,
                any,
            } => self.sql_like_to_expr(
                negated,
                *expr,
                *pattern,
                escape_char,
                schema,
                planner_context,
                false,
                any,
            ),

            SQLExpr::ILike {
                negated,
                expr,
                pattern,
                escape_char,
                any,
            } => self.sql_like_to_expr(
                negated,
                *expr,
                *pattern,
                escape_char,
                schema,
                planner_context,
                true,
                any,
            ),

            SQLExpr::SimilarTo {
                negated,
                expr,
                pattern,
                escape_char,
            } => self.sql_similarto_to_expr(
                negated,
                *expr,
                *pattern,
                escape_char,
                schema,
                planner_context,
            ),

            SQLExpr::BinaryOp { .. } => {
                internal_err!("binary_op should be handled by sql_expr_to_logical_expr.")
            }

            #[cfg(feature = "unicode_expressions")]
            SQLExpr::Substring {
                expr,
                substring_from,
                substring_for,
                special: _,
            } => self.sql_substring_to_expr(
                expr,
                substring_from,
                substring_for,
                schema,
                planner_context,
            ),

            #[cfg(not(feature = "unicode_expressions"))]
            SQLExpr::Substring { .. } => {
                internal_err!(
                    "statement substring requires compilation with feature flag: unicode_expressions."
                )
            }

            SQLExpr::Trim {
                expr,
                trim_where,
                trim_what,
                trim_characters,
            } => self.sql_trim_to_expr(
                *expr,
                trim_where,
                trim_what,
                trim_characters,
                schema,
                planner_context,
            ),

            SQLExpr::Function(function) => {
                // workaround for https://github.com/apache/datafusion-sqlparser-rs/issues/1909
                if matches!(function.args, FunctionArguments::None)
                    && function.name.0.len() > 1
                    && function.name.0.iter().all(|part| part.as_ident().is_some())
                {
                    let ids = function
                        .name
                        .0
                        .iter()
                        .map(|part| part.as_ident().expect("just checked").clone())
                        .collect();
                    self.sql_compound_identifier_to_expr(ids, schema, planner_context)
                } else {
                    self.sql_function_to_expr(function, schema, planner_context)
                }
            }

            SQLExpr::Rollup(exprs) => {
                self.sql_rollup_to_expr(exprs, schema, planner_context)
            }
            SQLExpr::Cube(exprs) => self.sql_cube_to_expr(exprs, schema, planner_context),
            SQLExpr::GroupingSets(exprs) => {
                self.sql_grouping_sets_to_expr(exprs, schema, planner_context)
            }

            SQLExpr::Floor {
                expr,
                field: _field,
            } => self.sql_fn_name_to_expr(*expr, "floor", schema, planner_context),
            SQLExpr::Ceil {
                expr,
                field: _field,
            } => self.sql_fn_name_to_expr(*expr, "ceil", schema, planner_context),
            SQLExpr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => self.sql_overlay_to_expr(
                *expr,
                *overlay_what,
                *overlay_from,
                overlay_for,
                schema,
                planner_context,
            ),
            SQLExpr::Nested(e) => {
                self.sql_expr_to_logical_expr(*e, schema, planner_context)
            }

            SQLExpr::Exists { subquery, negated } => {
                self.parse_exists_subquery(*subquery, negated, schema, planner_context)
            }
            SQLExpr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                self.parse_in_subquery(*expr, *subquery, negated, schema, planner_context)
            }
            SQLExpr::Subquery(subquery) => {
                self.parse_scalar_subquery(*subquery, schema, planner_context)
            }

            SQLExpr::Struct { values, fields } => {
                self.parse_struct(schema, planner_context, values, fields)
            }
            SQLExpr::Position { expr, r#in } => {
                self.sql_position_to_expr(*expr, *r#in, schema, planner_context)
            }
            SQLExpr::AtTimeZone {
                timestamp,
                time_zone,
            } => Ok(Expr::Cast(Cast::new(
                Box::new(self.sql_expr_to_logical_expr_internal(
                    *timestamp,
                    schema,
                    planner_context,
                )?),
                match *time_zone {
                    SQLExpr::Value(ValueWithSpan {
                        value: Value::SingleQuotedString(s),
                        span: _,
                    }) => DataType::Timestamp(TimeUnit::Nanosecond, Some(s.into())),
                    _ => {
                        return not_impl_err!(
                            "Unsupported ast node in sqltorel: {time_zone:?}"
                        )
                    }
                },
            ))),
            SQLExpr::Dictionary(fields) => {
                self.try_plan_dictionary_literal(fields, schema, planner_context)
            }
            SQLExpr::Map(map) => {
                self.try_plan_map_literal(map.entries, schema, planner_context)
            }
            SQLExpr::AnyOp {
                left,
                compare_op,
                right,
                // ANY/SOME are equivalent, this field specifies which the user
                // specified but it doesn't affect the plan so ignore the field
                is_some: _,
            } => {
                let mut binary_expr = RawBinaryExpr {
                    op: compare_op,
                    left: self.sql_expr_to_logical_expr(
                        *left,
                        schema,
                        planner_context,
                    )?,
                    right: self.sql_expr_to_logical_expr(
                        *right,
                        schema,
                        planner_context,
                    )?,
                };
                for planner in self.context_provider.get_expr_planners() {
                    match planner.plan_any(binary_expr)? {
                        PlannerResult::Planned(expr) => {
                            return Ok(expr);
                        }
                        PlannerResult::Original(expr) => {
                            binary_expr = expr;
                        }
                    }
                }
                not_impl_err!("AnyOp not supported by ExprPlanner: {binary_expr:?}")
            }
            #[expect(deprecated)]
            SQLExpr::Wildcard(_token) => Ok(Expr::Wildcard {
                qualifier: None,
                options: Box::new(WildcardOptions::default()),
            }),
            #[expect(deprecated)]
            SQLExpr::QualifiedWildcard(object_name, _token) => Ok(Expr::Wildcard {
                qualifier: Some(self.object_name_to_table_reference(object_name)?),
                options: Box::new(WildcardOptions::default()),
            }),
            SQLExpr::Tuple(values) => self.parse_tuple(schema, planner_context, values),
            _ => not_impl_err!("Unsupported ast node in sqltorel: {sql:?}"),
        }
    }

    /// Parses a struct(..) expression and plans it creation
    fn parse_struct(
        &self,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
        values: Vec<SQLExpr>,
        fields: Vec<StructField>,
    ) -> Result<Expr> {
        if !fields.is_empty() {
            return not_impl_err!("Struct fields are not supported yet");
        }
        let is_named_struct = values
            .iter()
            .any(|value| matches!(value, SQLExpr::Named { .. }));

        let mut create_struct_args = if is_named_struct {
            self.create_named_struct_expr(values, schema, planner_context)?
        } else {
            self.create_struct_expr(values, schema, planner_context)?
        };

        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_struct_literal(create_struct_args, is_named_struct)? {
                PlannerResult::Planned(expr) => return Ok(expr),
                PlannerResult::Original(args) => create_struct_args = args,
            }
        }
        not_impl_err!("Struct not supported by ExprPlanner: {create_struct_args:?}")
    }

    fn parse_tuple(
        &self,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
        values: Vec<SQLExpr>,
    ) -> Result<Expr> {
        match values.first() {
            Some(SQLExpr::Identifier(_))
            | Some(SQLExpr::Value(_))
            | Some(SQLExpr::CompoundIdentifier(_)) => {
                self.parse_struct(schema, planner_context, values, vec![])
            }
            None => not_impl_err!("Empty tuple not supported yet"),
            _ => {
                not_impl_err!("Only identifiers and literals are supported in tuples")
            }
        }
    }

    fn sql_position_to_expr(
        &self,
        substr_expr: SQLExpr,
        str_expr: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let substr =
            self.sql_expr_to_logical_expr(substr_expr, schema, planner_context)?;
        let fullstr = self.sql_expr_to_logical_expr(str_expr, schema, planner_context)?;
        let mut position_args = vec![fullstr, substr];
        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_position(position_args)? {
                PlannerResult::Planned(expr) => return Ok(expr),
                PlannerResult::Original(args) => {
                    position_args = args;
                }
            }
        }

        not_impl_err!("Position not supported by ExprPlanner: {position_args:?}")
    }

    fn try_plan_dictionary_literal(
        &self,
        fields: Vec<DictionaryField>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let mut keys = vec![];
        let mut values = vec![];
        for field in fields {
            let key = lit(field.key.value);
            let value =
                self.sql_expr_to_logical_expr(*field.value, schema, planner_context)?;
            keys.push(key);
            values.push(value);
        }

        let mut raw_expr = RawDictionaryExpr { keys, values };

        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_dictionary_literal(raw_expr, schema)? {
                PlannerResult::Planned(expr) => {
                    return Ok(expr);
                }
                PlannerResult::Original(expr) => raw_expr = expr,
            }
        }
        not_impl_err!("Dictionary not supported by ExprPlanner: {raw_expr:?}")
    }

    fn try_plan_map_literal(
        &self,
        entries: Vec<MapEntry>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let mut exprs: Vec<_> = entries
            .into_iter()
            .flat_map(|entry| vec![entry.key, entry.value].into_iter())
            .map(|expr| self.sql_expr_to_logical_expr(*expr, schema, planner_context))
            .collect::<Result<Vec<_>>>()?;
        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_make_map(exprs)? {
                PlannerResult::Planned(expr) => {
                    return Ok(expr);
                }
                PlannerResult::Original(expr) => exprs = expr,
            }
        }
        not_impl_err!("MAP not supported by ExprPlanner: {exprs:?}")
    }

    // Handles a call to struct(...) where the arguments are named. For example
    // `struct (v as foo, v2 as bar)` by creating a call to the `named_struct` function
    fn create_named_struct_expr(
        &self,
        values: Vec<SQLExpr>,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Expr>> {
        Ok(values
            .into_iter()
            .enumerate()
            .map(|(i, value)| {
                let args = if let SQLExpr::Named { expr, name } = value {
                    [
                        name.value.lit(),
                        self.sql_expr_to_logical_expr(
                            *expr,
                            input_schema,
                            planner_context,
                        )?,
                    ]
                } else {
                    [
                        format!("c{i}").lit(),
                        self.sql_expr_to_logical_expr(
                            value,
                            input_schema,
                            planner_context,
                        )?,
                    ]
                };

                Ok(args)
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect())
    }

    // Handles a call to struct(...) where the arguments are not named. For example
    // `struct (v, v2)` by creating a call to the `struct` function
    // which will create a struct with fields named `c0`, `c1`, etc.
    fn create_struct_expr(
        &self,
        values: Vec<SQLExpr>,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Expr>> {
        values
            .into_iter()
            .map(|value| {
                self.sql_expr_to_logical_expr(value, input_schema, planner_context)
            })
            .collect::<Result<Vec<_>>>()
    }

    fn sql_in_list_to_expr(
        &self,
        expr: SQLExpr,
        list: Vec<SQLExpr>,
        negated: bool,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let list_expr = list
            .into_iter()
            .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
            .collect::<Result<Vec<_>>>()?;

        Ok(Expr::InList(InList::new(
            Box::new(self.sql_expr_to_logical_expr(expr, schema, planner_context)?),
            list_expr,
            negated,
        )))
    }

    #[allow(clippy::too_many_arguments)]
    fn sql_like_to_expr(
        &self,
        negated: bool,
        expr: SQLExpr,
        pattern: SQLExpr,
        escape_char: Option<String>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
        case_insensitive: bool,
        any: bool,
    ) -> Result<Expr> {
        if any {
            return not_impl_err!("ANY in LIKE expression");
        }
        let pattern = self.sql_expr_to_logical_expr(pattern, schema, planner_context)?;
        let escape_char = if let Some(char) = escape_char {
            if char.len() != 1 {
                return plan_err!("Invalid escape character in LIKE expression");
            }
            Some(char.chars().next().unwrap())
        } else {
            None
        };
        Ok(Expr::Like(Like::new(
            negated,
            Box::new(self.sql_expr_to_logical_expr(expr, schema, planner_context)?),
            Box::new(pattern),
            escape_char,
            case_insensitive,
        )))
    }

    fn sql_similarto_to_expr(
        &self,
        negated: bool,
        expr: SQLExpr,
        pattern: SQLExpr,
        escape_char: Option<String>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let pattern = self.sql_expr_to_logical_expr(pattern, schema, planner_context)?;
        let pattern_type = pattern.get_type(schema)?;
        if pattern_type != DataType::Utf8 && pattern_type != DataType::Null {
            return plan_err!("Invalid pattern in SIMILAR TO expression");
        }
        let escape_char = if let Some(char) = escape_char {
            if char.len() != 1 {
                return plan_err!("Invalid escape character in SIMILAR TO expression");
            }
            Some(char.chars().next().unwrap())
        } else {
            None
        };
        Ok(Expr::SimilarTo(Like::new(
            negated,
            Box::new(self.sql_expr_to_logical_expr(expr, schema, planner_context)?),
            Box::new(pattern),
            escape_char,
            false,
        )))
    }

    fn sql_trim_to_expr(
        &self,
        expr: SQLExpr,
        trim_where: Option<TrimWhereField>,
        trim_what: Option<Box<SQLExpr>>,
        trim_characters: Option<Vec<SQLExpr>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let arg = self.sql_expr_to_logical_expr(expr, schema, planner_context)?;
        let args = match (trim_what, trim_characters) {
            (Some(to_trim), None) => {
                let to_trim =
                    self.sql_expr_to_logical_expr(*to_trim, schema, planner_context)?;
                Ok(vec![arg, to_trim])
            }
            (None, Some(trim_characters)) => {
                if let Some(first) = trim_characters.first() {
                    let to_trim = self.sql_expr_to_logical_expr(
                        first.clone(),
                        schema,
                        planner_context,
                    )?;
                    Ok(vec![arg, to_trim])
                } else {
                    plan_err!("TRIM CHARACTERS cannot be empty")
                }
            }
            (Some(_), Some(_)) => {
                plan_err!("Both TRIM and TRIM CHARACTERS cannot be specified")
            }
            (None, None) => Ok(vec![arg]),
        }?;

        let fun_name = match trim_where {
            Some(TrimWhereField::Leading) => "ltrim",
            Some(TrimWhereField::Trailing) => "rtrim",
            Some(TrimWhereField::Both) => "btrim",
            None => "trim",
        };
        let fun = self
            .context_provider
            .get_function_meta(fun_name)
            .ok_or_else(|| {
                internal_datafusion_err!("Unable to find expected '{fun_name}' function")
            })?;

        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(fun, args)))
    }

    fn sql_overlay_to_expr(
        &self,
        expr: SQLExpr,
        overlay_what: SQLExpr,
        overlay_from: SQLExpr,
        overlay_for: Option<Box<SQLExpr>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let arg = self.sql_expr_to_logical_expr(expr, schema, planner_context)?;
        let what_arg =
            self.sql_expr_to_logical_expr(overlay_what, schema, planner_context)?;
        let from_arg =
            self.sql_expr_to_logical_expr(overlay_from, schema, planner_context)?;
        let mut overlay_args = match overlay_for {
            Some(for_expr) => {
                let for_expr =
                    self.sql_expr_to_logical_expr(*for_expr, schema, planner_context)?;
                vec![arg, what_arg, from_arg, for_expr]
            }
            None => vec![arg, what_arg, from_arg],
        };
        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_overlay(overlay_args)? {
                PlannerResult::Planned(expr) => return Ok(expr),
                PlannerResult::Original(args) => overlay_args = args,
            }
        }
        not_impl_err!("Overlay not supported by ExprPlanner: {overlay_args:?}")
    }

    fn sql_cast_to_expr(
        &self,
        expr: SQLExpr,
        data_type: SQLDataType,
        format: Option<CastFormat>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        if let Some(format) = format {
            return not_impl_err!("CAST with format is not supported: {format}");
        }

        let dt = self.convert_data_type(&data_type)?;
        let expr = self.sql_expr_to_logical_expr(expr, schema, planner_context)?;

        // numeric constants are treated as seconds (rather as nanoseconds)
        // to align with postgres / duckdb semantics
        let expr = match &dt {
            DataType::Timestamp(TimeUnit::Nanosecond, tz)
                if expr.get_type(schema)? == DataType::Int64 =>
            {
                Expr::Cast(Cast::new(
                    Box::new(expr),
                    DataType::Timestamp(TimeUnit::Second, tz.clone()),
                ))
            }
            _ => expr,
        };

        Ok(Expr::Cast(Cast::new(Box::new(expr), dt)))
    }

    /// Extracts the root expression and access chain from a compound expression.
    ///
    /// This function attempts to identify if a compound expression (like `a.b.c`) should be treated
    /// as a column reference with a qualifier (like `table.column`) or as a field access expression.
    ///
    /// # Arguments
    ///
    /// * `root` - The root SQL expression (e.g., the first part of `a.b.c`)
    /// * `access_chain` - Vector of access expressions (e.g., `.b` and `.c` parts)
    /// * `schema` - The schema to resolve column references against
    /// * `planner_context` - Context for planning expressions
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// * The resolved root expression
    /// * The remaining access chain that should be processed as field accesses
    fn extract_root_and_access_chain(
        &self,
        root: SQLExpr,
        mut access_chain: Vec<AccessExpr>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<(Expr, Vec<AccessExpr>)> {
        let SQLExpr::Identifier(root_ident) = root else {
            let root = self.sql_expr_to_logical_expr(root, schema, planner_context)?;
            return Ok((root, access_chain));
        };

        let mut compound_idents = vec![root_ident];
        let first_non_ident = access_chain
            .iter()
            .position(|access| !matches!(access, AccessExpr::Dot(SQLExpr::Identifier(_))))
            .unwrap_or(access_chain.len());
        for access in access_chain.drain(0..first_non_ident) {
            if let AccessExpr::Dot(SQLExpr::Identifier(ident)) = access {
                compound_idents.push(ident);
            } else {
                return internal_err!("Expected identifier in access chain");
            }
        }

        let root = if compound_idents.len() == 1 {
            self.sql_identifier_to_expr(
                compound_idents.pop().unwrap(),
                schema,
                planner_context,
            )?
        } else {
            self.sql_compound_identifier_to_expr(
                compound_idents,
                schema,
                planner_context,
            )?
        };
        Ok((root, access_chain))
    }

    fn sql_compound_field_access_to_expr(
        &self,
        root: SQLExpr,
        access_chain: Vec<AccessExpr>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let (root, access_chain) = self.extract_root_and_access_chain(
            root,
            access_chain,
            schema,
            planner_context,
        )?;
        let fields = access_chain
            .into_iter()
            .map(|field| match field {
                AccessExpr::Subscript(subscript) => {
                    match subscript {
                        Subscript::Index { index } => {
                            // index can be a name, in which case it is a named field access
                            match index {
                                SQLExpr::Value(ValueWithSpan {
                                    value:
                                        Value::SingleQuotedString(s)
                                        | Value::DoubleQuotedString(s),
                                    span: _,
                                }) => Ok(Some(GetFieldAccess::NamedStructField {
                                    name: ScalarValue::from(s),
                                })),
                                SQLExpr::JsonAccess { .. } => {
                                    not_impl_err!("JsonAccess")
                                }
                                // otherwise treat like a list index
                                _ => Ok(Some(GetFieldAccess::ListIndex {
                                    key: Box::new(self.sql_expr_to_logical_expr(
                                        index,
                                        schema,
                                        planner_context,
                                    )?),
                                })),
                            }
                        }
                        Subscript::Slice {
                            lower_bound,
                            upper_bound,
                            stride,
                        } => {
                            // Means access like [:2]
                            let lower_bound = if let Some(lower_bound) = lower_bound {
                                self.sql_expr_to_logical_expr(
                                    lower_bound,
                                    schema,
                                    planner_context,
                                )
                            } else {
                                not_impl_err!("Slice subscript requires a lower bound")
                            }?;

                            // means access like [2:]
                            let upper_bound = if let Some(upper_bound) = upper_bound {
                                self.sql_expr_to_logical_expr(
                                    upper_bound,
                                    schema,
                                    planner_context,
                                )
                            } else {
                                not_impl_err!("Slice subscript requires an upper bound")
                            }?;

                            // stride, default to 1
                            let stride = if let Some(stride) = stride {
                                self.sql_expr_to_logical_expr(
                                    stride,
                                    schema,
                                    planner_context,
                                )?
                            } else {
                                lit(1i64)
                            };

                            Ok(Some(GetFieldAccess::ListRange {
                                start: Box::new(lower_bound),
                                stop: Box::new(upper_bound),
                                stride: Box::new(stride),
                            }))
                        }
                    }
                }
                AccessExpr::Dot(expr) => match expr {
                    SQLExpr::Value(ValueWithSpan {
                        value: Value::SingleQuotedString(s) | Value::DoubleQuotedString(s),
                        span    : _
                    }) => Ok(Some(GetFieldAccess::NamedStructField {
                        name: ScalarValue::from(s),
                    })),
                    _ => {
                        not_impl_err!(
                            "Dot access not supported for non-string expr: {expr:?}"
                        )
                    }
                },
            })
            .collect::<Result<Vec<_>>>()?;

        fields
            .into_iter()
            .flatten()
            .try_fold(root, |expr, field_access| {
                let mut field_access_expr = RawFieldAccessExpr { expr, field_access };
                for planner in self.context_provider.get_expr_planners() {
                    match planner.plan_field_access(field_access_expr, schema)? {
                        PlannerResult::Planned(expr) => return Ok(expr),
                        PlannerResult::Original(expr) => {
                            field_access_expr = expr;
                        }
                    }
                }
                not_impl_err!(
                    "GetFieldAccess not supported by ExprPlanner: {field_access_expr:?}"
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use datafusion_common::config::ConfigOptions;
    use datafusion_common::TableReference;
    use datafusion_expr::logical_plan::builder::LogicalTableSource;
    use datafusion_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF};

    use super::*;

    struct TestContextProvider {
        options: ConfigOptions,
        tables: HashMap<String, Arc<dyn TableSource>>,
    }

    impl TestContextProvider {
        pub fn new() -> Self {
            let mut tables = HashMap::new();
            tables.insert(
                "table1".to_string(),
                create_table_source(vec![Field::new(
                    "column1".to_string(),
                    DataType::Utf8,
                    false,
                )]),
            );

            Self {
                options: Default::default(),
                tables,
            }
        }
    }

    impl ContextProvider for TestContextProvider {
        fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
            match self.tables.get(name.table()) {
                Some(table) => Ok(Arc::clone(table)),
                _ => plan_err!("Table not found: {}", name.table()),
            }
        }

        fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
            None
        }

        fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
            match name {
                "sum" => Some(datafusion_functions_aggregate::sum::sum_udaf()),
                _ => None,
            }
        }

        fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
            None
        }

        fn options(&self) -> &ConfigOptions {
            &self.options
        }

        fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
            None
        }

        fn udf_names(&self) -> Vec<String> {
            Vec::new()
        }

        fn udaf_names(&self) -> Vec<String> {
            vec!["sum".to_string()]
        }

        fn udwf_names(&self) -> Vec<String> {
            Vec::new()
        }
    }

    fn create_table_source(fields: Vec<Field>) -> Arc<dyn TableSource> {
        Arc::new(LogicalTableSource::new(Arc::new(
            Schema::new_with_metadata(fields, HashMap::new()),
        )))
    }

    macro_rules! test_stack_overflow {
        ($num_expr:expr) => {
            paste::item! {
                #[test]
                fn [<test_stack_overflow_ $num_expr>]() {
                    let schema = DFSchema::empty();
                    let mut planner_context = PlannerContext::default();

                    let expr_str = (0..$num_expr)
                        .map(|i| format!("column1 = 'value{:?}'", i))
                        .collect::<Vec<String>>()
                        .join(" OR ");

                    let dialect = GenericDialect{};
                    let mut parser = Parser::new(&dialect)
                        .try_with_sql(expr_str.as_str())
                        .unwrap();
                    let sql_expr = parser.parse_expr().unwrap();

                    let context_provider = TestContextProvider::new();
                    let sql_to_rel = SqlToRel::new(&context_provider);

                    // Should not stack overflow
                    sql_to_rel.sql_expr_to_logical_expr(
                        sql_expr,
                        &schema,
                        &mut planner_context,
                    ).unwrap();
                }
            }
        };
    }

    test_stack_overflow!(64);
    test_stack_overflow!(128);
    test_stack_overflow!(256);
    test_stack_overflow!(512);
    test_stack_overflow!(1024);
    test_stack_overflow!(2048);
    test_stack_overflow!(4096);
    test_stack_overflow!(8192);
    #[test]
    fn test_sql_to_expr_with_alias() {
        let schema = DFSchema::empty();
        let mut planner_context = PlannerContext::default();

        let expr_str = "SUM(int_col) as sum_int_col";

        let dialect = GenericDialect {};
        let mut parser = Parser::new(&dialect).try_with_sql(expr_str).unwrap();
        // from sqlparser
        let sql_expr = parser.parse_expr_with_alias().unwrap();

        let context_provider = TestContextProvider::new();
        let sql_to_rel = SqlToRel::new(&context_provider);

        let expr = sql_to_rel
            .sql_expr_to_logical_expr_with_alias(sql_expr, &schema, &mut planner_context)
            .unwrap();

        assert!(matches!(expr, Expr::Alias(_)));
    }
}
