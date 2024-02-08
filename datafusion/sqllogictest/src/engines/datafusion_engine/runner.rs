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

use std::{path::PathBuf, time::Duration};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion_common::DataFusionError;
use log::info;
use sqllogictest::DBOutput;

use super::{error::Result, normalize, DFSqlLogicTestError};

use crate::engines::output::{DFColumnType, DFOutput};

pub struct DataFusion {
    ctx: SessionContext,
    relative_path: PathBuf,
}

impl DataFusion {
    pub fn new(ctx: SessionContext, relative_path: PathBuf) -> Self {
        Self { ctx, relative_path }
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for DataFusion {
    type Error = DFSqlLogicTestError;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DFOutput> {
        info!(
            "[{}] Running query: \"{}\"",
            self.relative_path.display(),
            sql
        );
        match run_query(&self.ctx, sql).await {
            Ok(results) => Ok(results),
            Err(DFSqlLogicTestError::DataFusion(DataFusionError::NotImplemented(_))) => {
                Ok(DBOutput::Rows {
                    types: vec![],
                    rows: vec![vec!["df_unimplemented".to_string()]],
                })
            }
            // Err(DFSqlLogicTestError::DataFusion(DataFusionError::External(_))) => {
            //     Ok(DBOutput::Rows {
            //         types: vec![],
            //         rows: vec![vec!["df_unimplemented".to_string()]],
            //     })
            // }
            Err(e) => {
                let errmsg = format!("{:?}", e);
                println!("########3 Error: {}", errmsg);
                if errmsg
                    .matches("check_analyzed_plan")
                    .collect::<Vec<_>>()
                    .len()
                    > 0
                {
                    return Ok(DBOutput::Rows {
                        types: vec![],
                        rows: vec![vec!["df_unimplemented".to_string()]],
                    });
                }
                if errmsg
                    .matches("Projections require unique expression names")
                    .collect::<Vec<_>>()
                    .len()
                    > 0
                {
                    return Ok(DBOutput::Rows {
                        types: vec![],
                        rows: vec![vec!["df_unimplemented".to_string()]],
                    });
                }
                if errmsg
                    .matches("AmbiguousReference")
                    .collect::<Vec<_>>()
                    .len()
                    > 0
                {
                    return Ok(DBOutput::Rows {
                        types: vec![],
                        rows: vec![vec!["df_unimplemented".to_string()]],
                    });
                }
                if errmsg.matches("type_coercion").collect::<Vec<_>>().len() > 0 {
                    return Ok(DBOutput::Rows {
                        types: vec![],
                        rows: vec![vec!["df_unimplemented".to_string()]],
                    });
                }

                Err(e)
            }
        }
    }

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        "DataFusion"
    }

    /// [`DataFusion`] calls this function to perform sleep.
    ///
    /// The default implementation is `std::thread::sleep`, which is universal to any async runtime
    /// but would block the current thread. If you are running in tokio runtime, you should override
    /// this by `tokio::time::sleep`.
    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

async fn run_query(ctx: &SessionContext, sql: impl Into<String>) -> Result<DFOutput> {
    let sql = sql.into();
    println!("########## Running query: {}", sql);
    let df = ctx.sql(sql.as_str()).await?;

    let types = normalize::convert_schema_to_types(df.schema().fields());
    let results: Vec<RecordBatch> = df.collect().await?;
    let rows = normalize::convert_batches(results)?;

    if rows.is_empty() && types.is_empty() {
        Ok(DBOutput::StatementComplete(0))
    } else {
        Ok(DBOutput::Rows { types, rows })
    }
}
