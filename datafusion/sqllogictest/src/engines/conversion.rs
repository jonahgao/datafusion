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

use arrow::datatypes::DecimalType;
use bigdecimal::num_traits::Float;
use bigdecimal::BigDecimal;
use std::str::FromStr;

#[cfg(feature = "postgres")]
use rust_decimal::Decimal as PgDecimal;

/// Represents a constant for NULL string in your database.
pub const NULL_STR: &str = "NULL";

pub(crate) fn bool_to_str(value: bool) -> String {
    if value {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

pub(crate) fn varchar_to_str(value: &str) -> String {
    if value.is_empty() {
        "(empty)".to_string()
    } else {
        value.trim_end_matches('\n').to_string()
    }
}

pub(crate) fn float_to_str<T: Float + ToString>(value: T) -> String {
    if value.is_nan() {
        // The sign of NaN can be different depending on platform.
        // So the string representation of NaN ignores the sign.
        "NaN".to_string()
    } else if value.is_infinite() {
        if value.is_sign_positive() {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        }
    } else {
        round_float_str(value.to_string())
    }
}

pub(crate) fn arrow_decimal_to_str<T: DecimalType>(
    value: T::Native,
    precision: &u8,
    scale: &i8,
) -> String {
    let str = T::format_decimal(value, *precision, *scale);
    BigDecimal::from_str(&str)
        .unwrap()
        .normalized()
        .to_plain_string()
}

#[cfg(feature = "postgres")]
pub(crate) fn pg_decimal_to_str(value: PgDecimal) -> String {
    // In Postgres, the `AVG()` function returns a NUMERIC type, whereas
    // in DataFusion it returns a Float64. To make the results comparable,
    // treat PostgreSQL NUMERIC as Float64.
    round_float_str(value.to_string())
}

/// In order to eliminate differences across various platforms,
/// truncate and round float values to 12 digits after the decimal point.
fn round_float_str(value: String) -> String {
    let dec = BigDecimal::from_str(&value).unwrap();
    dec.round(12).normalized().to_plain_string()
}
