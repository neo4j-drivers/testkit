use std::cell::LazyCell;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use anyhow::anyhow;
use itertools::Itertools;
use regex::Regex;
use serde_json::{Map as JsonMap, Value as JsonValue};

use super::{load_json_values, parse_jolt_bytes, parse_jolt_sigil, ActorConfig, Result};
use crate::bolt_version::JoltVersion;
use crate::context::Context;
use crate::ext::serde_json as serde_json_ext;
use crate::jolt::JoltSigil;
use crate::parse_error::ParseError;
use crate::util::opt_res_ret;
use crate::values::bolt_struct::{
    JoltDate, JoltDateTime, JoltDuration, JoltPoint, JoltTime, JoltVector, JoltVectorType,
    TAG_DATE, TAG_DURATION, TAG_LOCAL_TIME, TAG_POINT_2D, TAG_POINT_3D, TAG_TIME, TAG_VECTOR,
};
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

pub type ValidateValueFn =
    Box<dyn Fn(&PackStreamValue) -> anyhow::Result<()> + 'static + Send + Sync>;
pub type ValidateValuesFn =
    Box<dyn Fn(&[PackStreamValue]) -> anyhow::Result<()> + 'static + Send + Sync>;

pub enum IsJoltValidator {
    Yes(ValidateValueFn),
    No(JsonMap<String, JsonValue>),
}

pub fn build_fields_validator(
    msg_body: Option<(Context, &str)>,
    config: &ActorConfig,
) -> Result<ValidateValuesFn> {
    let Some((ctx, line)) = msg_body else {
        return Ok(Box::new(build_no_fields_validator));
    };
    // RUN "RETURN $n AS n" {"n": 1}
    let field_validators: Vec<_> = load_json_values(line, ctx)?
        .into_iter()
        .map(|field| build_field_validator(field, config))
        .collect::<Result<_>>()?;

    Ok(Box::new(move |fields| {
        if fields.len() != field_validators.len() {
            return Err(anyhow!(
                "Expected {} fields, but found {}",
                fields.len(),
                field_validators.len()
            ));
        }
        for (field, field_validator) in fields.iter().zip(&field_validators) {
            field_validator(field)?;
        }
        Ok(())
    }))
}

pub fn build_no_fields_validator(message: &[PackStreamValue]) -> anyhow::Result<()> {
    match message.len() {
        0 => Ok(()),
        _ => Err(anyhow!("Expected 0 fields")),
    }
}

pub fn build_validator(
    expected: JsonMap<String, JsonValue>,
    config: &ActorConfig,
) -> Result<IsJoltValidator> {
    if expected.len() != 1 {
        return Ok(IsJoltValidator::No(expected));
    }
    let (sigil, expected) = expected.into_iter().next().expect("non-empty check above");
    let (versionless_sigil, jolt_version) = parse_jolt_sigil(&sigil, config)?;
    let Some(parsed_sigil) = JoltSigil::from_str(versionless_sigil) else {
        return Ok(IsJoltValidator::No(
            [(sigil, expected)].into_iter().collect(),
        ));
    };
    match parsed_sigil {
        JoltSigil::Bool => build_bool(expected, config),
        JoltSigil::Integer => build_int(expected, config),
        JoltSigil::Float => build_float(expected),
        JoltSigil::String => build_str(expected, config),
        JoltSigil::Bytes => build_bytes(expected),
        JoltSigil::List => build_list(expected, config),
        JoltSigil::Dict => build_dict(expected, config),
        JoltSigil::Temporal => build_temporal(expected, jolt_version),
        JoltSigil::Spatial => build_spatial(expected),
        JoltSigil::Node => build_node(&expected, &sigil),
        JoltSigil::RelationshipForward | JoltSigil::RelationshipBackward => {
            build_relationship(&expected, &sigil)
        }
        JoltSigil::Path => build_path(&expected, &sigil),
        JoltSigil::Vector => build_vector(expected, jolt_version, config),
        JoltSigil::UnsupportedType => build_unsupported_type(),
    }
}

macro_rules! match_any {
    ($typ:expr, $pattern:pat) => {
        return Ok(IsJoltValidator::Yes(Box::new(|msg| match msg {
            $pattern => Ok(()),
            _ => Err(anyhow!("Expected any {:} found {:?}", $typ, msg)),
        })))
    };
}

// try to build jolt matcher (incl. Structs like datetime)
//   * if has 1 key-value pair
//   * && key is known sigil
//   * Special string "*" applies: E.g., `C: RUN {"Z": "*"}` will match any integer: `C: RUN 1` and `C: RUN 2`, but not `C: RUN 1.2` or `C: RUN "*"`.
fn is_jolt_match_all(value: &JsonValue) -> bool {
    value.as_str().is_some_and(|s| s == "*")
}

fn build_bool(expected: JsonValue, config: &ActorConfig) -> Result<IsJoltValidator> {
    if is_jolt_match_all(&expected) {
        match_any!("bool", PackStreamValue::Boolean(_));
    }
    if !expected.is_boolean() {
        return Err(ParseError::new(format!(
            "Expected bool after sigil \"?\", but found {expected:?}",
        )));
    }
    Ok(IsJoltValidator::Yes(build_field_validator(
        expected, config,
    )?))
}

fn build_int(expected: JsonValue, config: &ActorConfig) -> Result<IsJoltValidator> {
    if is_jolt_match_all(&expected) {
        match_any!("integer", PackStreamValue::Integer(_));
    }
    let JsonValue::String(expected) = expected else {
        return Err(ParseError::new(format!(
            "Expected string after sigil \"Z\", but found {expected:?}",
        )));
    };
    let expected = i64::from_str(&expected).map_err(|e| {
        ParseError::new(format!(
            "Failed to parse i64 after sigil \"Z\": {expected:?} because {e}",
        ))
    })?;
    Ok(IsJoltValidator::Yes(build_field_validator(
        JsonValue::Number(expected.into()),
        config,
    )?))
}

fn build_float(expected: JsonValue) -> Result<IsJoltValidator> {
    if is_jolt_match_all(&expected) {
        match_any!("float", PackStreamValue::Float(_));
    }
    let JsonValue::String(expected) = expected else {
        return Err(ParseError::new(format!(
            "Expected string after sigil \"R\", but found {expected:?}",
        )));
    };
    let expected = f64::from_str(&expected).map_err(|e| {
        ParseError::new(format!(
            "Failed to parse f64 after sigil \"R\": {expected:?} because {e}",
        ))
    })?;
    Ok(IsJoltValidator::Yes(Box::new(move |msg| {
        if msg == &PackStreamValue::Float(expected) {
            Ok(())
        } else {
            Err(anyhow!("Expected {expected:?} found {msg:?}"))
        }
    })))
}

fn build_str(expected: JsonValue, config: &ActorConfig) -> Result<IsJoltValidator> {
    if is_jolt_match_all(&expected) {
        match_any!("string", PackStreamValue::String(_));
    }
    if !expected.is_string() {
        return Err(ParseError::new(format!(
            "Expected string after sigil \"U\", but found {expected:?}",
        )));
    }
    Ok(IsJoltValidator::Yes(build_field_validator(
        expected, config,
    )?))
}

fn build_bytes(expected: JsonValue) -> Result<IsJoltValidator> {
    if is_jolt_match_all(&expected) {
        match_any!("bytes", PackStreamValue::Bytes(_));
    }
    let bytes = parse_jolt_bytes(expected)?;
    Ok(IsJoltValidator::Yes(Box::new(move |msg| match msg {
        PackStreamValue::Bytes(received) if &bytes == received => Ok(()),
        _ => Err(anyhow!("Expected bytes {:?} found {:?}", &bytes, msg)),
    })))
}

fn build_list(expected: JsonValue, config: &ActorConfig) -> Result<IsJoltValidator> {
    if is_jolt_match_all(&expected) {
        match_any!("list", PackStreamValue::List(_));
    }
    if !expected.is_array() {
        return Err(ParseError::new(format!(
            "Expected array after sigil \"[]\", but found {expected:?}",
        )));
    }
    Ok(IsJoltValidator::Yes(build_field_validator(
        expected, config,
    )?))
}

fn build_dict(expected: JsonValue, config: &ActorConfig) -> Result<IsJoltValidator> {
    if is_jolt_match_all(&expected) {
        match_any!("map", PackStreamValue::Dict(_));
    }
    let JsonValue::Object(expected) = expected else {
        return Err(ParseError::new(format!(
            "Expected object after sigil \"{{}}\", but found {expected:?}",
        )));
    };
    Ok(IsJoltValidator::Yes(build_map_validator(expected, config)?))
}

fn build_temporal(expected: JsonValue, jolt_version: JoltVersion) -> Result<IsJoltValidator> {
    let JsonValue::String(expected) = expected else {
        return Err(ParseError::new(format!(
            "Expected temporal string after sigil \"T\", but found {expected:?}",
        )));
    };
    let validator = build_date_validator(&expected)
        .or_else(|| build_time_validator(&expected))
        .or_else(|| build_date_time_validator(&expected, jolt_version))
        .or_else(|| build_duration_validator(&expected))
        .transpose()?
        .ok_or_else(|| {
            ParseError::new(format!(
                "Expected temporal string after sigil \"T\", but found {expected:?}",
            ))
        })?;
    Ok(IsJoltValidator::Yes(validator))
}

fn build_spatial(expected: JsonValue) -> Result<IsJoltValidator> {
    if is_jolt_match_all(&expected) {
        return Ok(IsJoltValidator::Yes(Box::new(|msg| match msg {
            PackStreamValue::Struct(PackStreamStruct { tag, fields }) => {
                match (tag, fields.as_slice()) {
                    (
                        &TAG_POINT_2D,
                        [PackStreamValue::Integer(_), PackStreamValue::Float(_), PackStreamValue::Float(_)],
                    )
                    | (
                        &TAG_POINT_3D,
                        [PackStreamValue::Integer(_), PackStreamValue::Float(_), PackStreamValue::Float(_), PackStreamValue::Float(_)],
                    ) => Ok(()),
                    _ => Err(anyhow!("Expected any date struct found {msg:?}")),
                }
            }
            _ => Err(anyhow!("Expected any spatial struct found {msg:?}")),
        })));
    }
    let JsonValue::String(expected) = expected else {
        return Err(ParseError::new(format!(
            "Expected spatial string after sigil \"@\", but found {expected:?}",
        )));
    };
    let bolt_point = JoltPoint::parse(&expected)?;
    let expected = PackStreamValue::Struct(bolt_point.as_struct());
    Ok(IsJoltValidator::Yes(Box::new(move |msg| {
        if msg == &expected {
            Ok(())
        } else {
            Err(anyhow!("Expected {expected:?} found {msg:?}"))
        }
    })))
}

fn build_node(expected: &JsonValue, sigil: &str) -> Result<IsJoltValidator> {
    let fixed_syntax = format!(
        r#"{{"\{sigil}": {}}}"#,
        serde_json_ext::compact_pretty_print(expected)
            .expect("JsonValue cannot fail Json serialization")
    );
    Err(ParseError::new(format!(
        "Node structs cannot be received by the server, only sent. \
            If you meant to match a map, use `{fixed_syntax}` instead.",
    )))
}

fn build_relationship(expected: &JsonValue, sigil: &str) -> Result<IsJoltValidator> {
    let fixed_syntax = format!(
        r#"{{"\{sigil}": {}}}"#,
        serde_json_ext::compact_pretty_print(expected)
            .expect("JsonValue cannot fail Json serialization")
    );
    Err(ParseError::new(format!(
        "Relationship structs cannot be received by the server, only sent. \
            If you meant to match a map, use `{fixed_syntax}` instead.",
    )))
}

fn build_path(expected: &JsonValue, sigil: &str) -> Result<IsJoltValidator> {
    let fixed_syntax = format!(
        r#"{{"\{sigil}": {}}}"#,
        serde_json_ext::compact_pretty_print(expected)
            .expect("JsonValue cannot fail Json serialization")
    );
    Err(ParseError::new(format!(
        "Path structs cannot be received by the server, only sent. \
            If you meant to match a map, use `{fixed_syntax}` instead.",
    )))
}

fn build_vector(
    expected: JsonValue,
    jolt_version: JoltVersion,
    config: &ActorConfig,
) -> Result<IsJoltValidator> {
    if is_jolt_match_all(&expected) {
        return Ok(IsJoltValidator::Yes(Box::new(|msg| {
            fn fail(msg: &PackStreamValue) -> anyhow::Result<()> {
                Err(anyhow!("Expected any vector struct found {msg:?}"))
            }

            match msg {
                PackStreamValue::Struct(PackStreamStruct { tag, fields }) => {
                    match (tag, fields.as_slice()) {
                        (
                            &TAG_VECTOR,
                            [PackStreamValue::Bytes(type_marker), PackStreamValue::Bytes(data)],
                        ) => {
                            let [type_marker] = *type_marker.as_slice() else {
                                return fail(msg);
                            };
                            let Some(inner_type) =
                                JoltVectorType::from_packstream_marker(type_marker)
                            else {
                                return fail(msg);
                            };
                            if data.len() % inner_type.size() != 0 {
                                return fail(msg);
                            }
                            Ok(())
                        }
                        _ => fail(msg),
                    }
                }
                _ => fail(msg),
            }
        })));
    }
    let bolt_vector = JoltVector::parse(expected, jolt_version, config)?;
    let expected_struct = bolt_vector.into_struct();
    Ok(IsJoltValidator::Yes(build_struct_match_validator(
        expected_struct,
    )))
}

fn build_unsupported_type() -> Result<IsJoltValidator> {
    Err(ParseError::new(
        "UnsupportedType structs cannot be received by the server, only sent.",
    ))
}

pub fn build_field_validator(field: JsonValue, config: &ActorConfig) -> Result<ValidateValueFn> {
    Ok(match field {
        JsonValue::Null => Box::new(|msg| match msg {
            PackStreamValue::Null => Ok(()),
            _ => Err(anyhow!("Expected null")),
        }),
        JsonValue::Bool(expected) => Box::new(move |msg| match msg {
            PackStreamValue::Boolean(received) if received == &expected => Ok(()),
            _ => Err(anyhow!("Expected {expected:?} found {msg:?}")),
        }),
        JsonValue::Number(expected) if expected.is_i64() => {
            let expected = expected.as_i64().expect("checked in match arm");
            Box::new(move |msg| match msg {
                PackStreamValue::Integer(received) if received == &expected => Ok(()),
                _ => Err(anyhow!("Expected {expected:?} found {msg:?}")),
            })
        }
        JsonValue::Number(expected) if expected.is_f64() => {
            let expected = expected.as_f64().expect("checked in match arm");
            assert!(
                expected.is_finite(),
                "json does not allow for NaN or Inf. Therefore, we don't handle those here",
            );
            Box::new(move |msg| match msg {
                #[allow(clippy::float_cmp, reason = "this is not about arithmetic")]
                PackStreamValue::Float(received) if received == &expected => Ok(()),
                _ => Err(anyhow!("Expected {expected:?} found {msg:?}")),
            })
        }
        JsonValue::Number(expected) => {
            return Err(ParseError::new(format!(
                "Can't parse number (must be i64 or f64) {expected:?}",
            )));
        }
        JsonValue::String(expected) => Box::new(move |msg| match msg {
            _ if expected == "*" => Ok(()),
            PackStreamValue::String(received) => validate_str_field_eq(&expected, received),
            _ => Err(anyhow!("Expected {expected:?} found {msg:?}")),
        }),
        JsonValue::Array(expected) => {
            let validators = expected
                .into_iter()
                .map(|value| build_field_validator(value, config))
                .collect::<Result<Vec<_>>>()?;
            Box::new(move |msg| {
                let PackStreamValue::List(received) = msg else {
                    return Err(anyhow!("Expected list, found {msg:?}"));
                };
                if validators.len() != received.len() {
                    return Err(anyhow!(
                        "Expected {} fields, found {}",
                        validators.len(),
                        received.len()
                    ));
                }
                for (validator, received) in validators.iter().zip(received.iter()) {
                    validator(received)?;
                }
                Ok(())
            })
        }
        JsonValue::Object(expected) => {
            let expected = match build_validator(expected, config)? {
                IsJoltValidator::Yes(jolt_validator) => return Ok(jolt_validator),
                IsJoltValidator::No(expected) => expected,
            };

            build_map_validator(expected, config)?
        }
    })
}

fn validate_str_field_eq(expected: &str, received: &str) -> anyhow::Result<()> {
    #[inline]
    fn match_(expected: &str, received: &str) -> anyhow::Result<()> {
        if expected == received {
            Ok(())
        } else {
            Err(anyhow!("Expected {expected:?} found {received:?}"))
        }
    }
    if expected.contains('\\') {
        let escaped = expected.replace(r"\\", r"\");
        match_(&escaped, received)
    } else {
        match_(expected, received)
    }
}

fn build_map_validator(
    expected: JsonMap<String, JsonValue>,
    config: &ActorConfig,
) -> Result<ValidateValueFn> {
    let mut required_keys = HashSet::new();
    let mut unique_keys = HashSet::new();
    let validators = expected
        .into_iter()
        .map(|(key, expect_for_key)| {
            let (key, validator, req) = build_map_entry_validator(&key, expect_for_key, config)?;
            if req {
                required_keys.insert(key.clone());
            }
            if !unique_keys.insert(key.clone()) {
                return Err(ParseError::new(format!(
                    "contains same unescaped key twice {key}"
                )));
            }
            Ok((key, validator))
        })
        .collect::<Result<HashMap<_, _>>>()?;

    Ok(Box::new(move |msg| {
        let PackStreamValue::Dict(received) = msg else {
            return Err(anyhow!("Expected map, found {msg:?}"));
        };

        let mut matched_keys = HashSet::new();

        for (key, value) in received {
            let Some(validator) = validators.get(key) else {
                return Err(anyhow!("Unexpected Key:{key} was received."));
            };
            validator(value)?;
            matched_keys.insert(key.clone());
        }

        if matched_keys.is_superset(&required_keys) {
            Ok(())
        } else {
            let mut missing_keys = required_keys.difference(&matched_keys);
            Err(anyhow!(
                "Received message with missing keys: {:?}",
                missing_keys.join(", ")
            ))
        }
    }))
}

/// # Returns
///  * String: the map key this validator should be applied to with markers stripped from it.
///  * `ValidateValueFn`: the validator function
///  * bool: whether the key is required (`true`), or optional (`false`).
fn build_map_entry_validator(
    key: &str,
    expected: JsonValue,
    config: &ActorConfig,
) -> Result<(String, ValidateValueFn, bool)> {
    //  JSON object keys:
    //    * The key will be **unescaped** before it is matched:
    //      `\\`, `\[`, `\]`, `\{`, and `\}` are turned into `\`, `[`, `]`, `{`, and `}` respectively.
    //    * If the escaped key starts with `[` and ends with `]`, the corresponding key/value pair is **optional**.
    //      E.g., `C: PULL {"[n]": 1000}` matches `C: PULL {}` and `C: PULL {"n": 1000}`, but not `C: PULL {"n": 1000, "m": 1001}`,  `C: PULL {"n": 1}`, or  `C: PULL null`.
    //    * If the escaped key ends on `{}` after potential optional-brackets (s. above) have been stripped, the corresponding value will be compared **sorted** if it's a list.
    //      E.g., `C: MSG {"foo{}": [1, 2]}` will match `C: MSG {"foo": [1, 2]}` and `C: MSG {"foo": [2, 1]}`, but `C: MSG {"foo{}": "ba"}` will not match `C: MSG {"foo": "ab"}`.
    //    * Example for **optional** and **sorted**: `C: MSG {"[foo{}]": [1, 2]}`.

    let key = ParsedMapKey::parse(key);
    let required = !key.is_optional;
    let ordered = key.is_ordered;
    let key = key.unescaped;
    let validator: ValidateValueFn = match expected {
        JsonValue::Array(expected) if ordered => {
            let validators = expected
                .into_iter()
                .map(|value| build_field_validator(value, config))
                .collect::<Result<Vec<_>>>()?;
            Box::new(move |msg| {
                let PackStreamValue::List(received) = msg else {
                    return Err(anyhow!("Expected list, found {msg:?}"));
                };
                if validators.len() != received.len() {
                    return Err(anyhow!(
                        "Expected {} fields, found {}",
                        validators.len(),
                        received.len()
                    ));
                }
                let mut left_validators = HashSet::new();
                left_validators.extend(0..validators.len());
                'values: for value_received in received {
                    for validator_idx in &left_validators {
                        let validator_idx = *validator_idx;
                        let validator = &validators[validator_idx];
                        if let Ok(()) = validator(value_received) {
                            left_validators.remove(&validator_idx);
                            continue 'values;
                        }
                    }
                    return Err(anyhow!(
                        "Unexpected value in any order array: {value_received:?}"
                    ));
                }
                Ok(())
            })
        }
        _ => build_field_validator(expected, config)?,
    };
    Ok((key, validator, required))
}

/// yes:
/// 2020-01-01
/// 2020-01
/// 2020
///
/// no:
/// 2020-1-1
/// --1
fn build_date_validator(s: &str) -> Option<Result<ValidateValueFn>> {
    if s == "*" {
        return Some(Ok(Box::new(|msg| match msg {
            PackStreamValue::Struct(PackStreamStruct { tag, fields }) => {
                match (tag, fields.as_slice()) {
                    (&TAG_DATE, [PackStreamValue::Integer(_)]) => Ok(()),
                    _ => Err(anyhow!("Expected any date struct found {msg:?}")),
                }
            }
            _ => Err(anyhow!("Expected any date struct found {msg:?}")),
        })));
    }
    let bolt_date = opt_res_ret!(JoltDate::parse(s));
    let expected_struct = bolt_date.as_struct();
    Some(Ok(build_struct_match_validator(expected_struct)))
}

/// yes:
/// 12:00:00.000000000+0000
/// 12:00:00.000+0000
/// 12:00:00+00:00
/// 12:00:00+00
/// 12:00:00Z
/// 12:00Z
/// 12Z
/// 12:00:00-01
///
/// no:
/// 12:00:00.0000000000Z
/// 12:00:00-0000
/// 12:0:0Z
/// 12:00:00+01:02:03
/// 12:00:00+00:00\[Europe/Berlin]
fn build_time_validator(s: &str) -> Option<Result<ValidateValueFn>> {
    if s == "*" {
        return Some(Ok(Box::new(|msg| match msg {
            PackStreamValue::Struct(PackStreamStruct { tag, fields }) => {
                match (tag, fields.as_slice()) {
                    (&TAG_LOCAL_TIME, [PackStreamValue::Integer(_)]) => Ok(()),
                    (&TAG_TIME, [PackStreamValue::Integer(_), PackStreamValue::Integer(_)]) => {
                        Ok(())
                    }
                    _ => Err(anyhow!("Expected any time struct found {msg:?}")),
                }
            }
            _ => Err(anyhow!("Expected any time struct found {msg:?}")),
        })));
    }
    let bolt_time = opt_res_ret!(JoltTime::parse(s));
    let expected_struct = opt_res_ret!(bolt_time.as_struct());
    Some(Ok(build_struct_match_validator(expected_struct)))
}

/// yes:
/// `<date_re>T<time_re><timezone_name>`
/// where `<date_re>` is anything that works for [`build_date_validator`], `<time_re>`
/// is anything that works for [`build_time_validator`] and `<timezone_name>` (optional) is any
/// timezone name in square brackets, e.g., `[Europe/Stockholm]`. `<timezone_name>` may only be
/// present, when `<time_re>` has a time offset.
///
/// no:
/// anything else
fn build_date_time_validator(
    s: &str,
    jolt_version: JoltVersion,
) -> Option<Result<ValidateValueFn>> {
    if s == "*" {
        return Some(Ok(Box::new(move |msg| match msg {
            PackStreamValue::Struct(PackStreamStruct { tag, fields }) => {
                match (jolt_version, tag, fields.as_slice()) {
                    (
                        JoltVersion::V1,
                        0x66,
                        [PackStreamValue::Integer(_), PackStreamValue::Integer(_), PackStreamValue::String(_)],
                    )
                    | (
                        JoltVersion::V2,
                        0x69,
                        [PackStreamValue::Integer(_), PackStreamValue::Integer(_), PackStreamValue::String(_)],
                    )
                    | (
                        JoltVersion::V1,
                        0x46,
                        [PackStreamValue::Integer(_), PackStreamValue::Integer(_), PackStreamValue::Integer(_)],
                    )
                    | (
                        JoltVersion::V2,
                        0x49,
                        [PackStreamValue::Integer(_), PackStreamValue::Integer(_), PackStreamValue::Integer(_)],
                    )
                    | (_, 0x64, [PackStreamValue::Integer(_), PackStreamValue::Integer(_)]) => {
                        Ok(())
                    }
                    _ => Err(anyhow!("Expected any date time struct found {msg:?}")),
                }
            }
            _ => Err(anyhow!("Expected any date time struct found {msg:?}")),
        })));
    }
    let bolt_date_time = opt_res_ret!(JoltDateTime::parse(s));
    let expected_struct = opt_res_ret!(bolt_date_time.into_struct(jolt_version));
    Some(Ok(build_struct_match_validator(expected_struct)))
}

/// yes:
/// P12Y13M40DT10H70M80.000000000S
/// P12Y-13M40DT-10H70M80.000000000S
/// P12Y
/// PT70M
/// P12DT10H70M
///
/// no:
/// P12Y13M40DT10H70M80.0000000000S
/// P12Y13M40DT10H70.1M10S
/// P5W
fn build_duration_validator(s: &str) -> Option<Result<ValidateValueFn>> {
    fn get_int_field(i: usize, name: &str, received: &PackStreamStruct) -> anyhow::Result<i64> {
        match received.fields.get(i) {
            None => Err(anyhow!(
                "Received invalid duration: {received:?} (missing {name})"
            )),
            Some(PackStreamValue::Integer(value)) => Ok(*value),
            Some(_) => Err(anyhow!(
                "Received invalid duration: {received:?} ({name} not integer)"
            )),
        }
    }

    if s == "*" {
        return Some(Ok(Box::new(|msg| match msg {
            PackStreamValue::Struct(PackStreamStruct { tag, fields }) => {
                match (tag, fields.as_slice()) {
                    (
                        &TAG_DURATION,
                        [PackStreamValue::Integer(_), PackStreamValue::Integer(_), PackStreamValue::Integer(_), PackStreamValue::Integer(_)],
                    ) => Ok(()),
                    _ => Err(anyhow!("Expected any duration struct found {msg:?}")),
                }
            }
            _ => Err(anyhow!("Expected any duration struct found {msg:?}")),
        })));
    }
    let JoltDuration {
        months,
        days,
        seconds,
        nanos,
    } = opt_res_ret!(JoltDuration::parse(s));
    let total_nanos = i128::from(seconds) * 1_000_000_000 + i128::from(nanos);
    Some(Ok(Box::new(move |msg| match msg {
        PackStreamValue::Struct(received) => {
            if received.tag != TAG_DURATION {
                return Err(anyhow!(
                    "Expected duration (tag {TAG_DURATION:#X}), found {msg:?}",
                ));
            }
            let received_months = get_int_field(0, "months", received)?;
            let received_days = get_int_field(1, "days", received)?;
            let received_seconds = get_int_field(2, "seconds", received)?;
            let received_nanos = get_int_field(3, "nanoseconds", received)?;
            let received_total_nanos =
                i128::from(received_seconds) * 1_000_000_000 + i128::from(received_nanos);
            if received_months != months {
                return Err(anyhow!(
                    "Expected duration months: {months}, found {received_months} in {received:?}",
                ));
            }
            if received_days != days {
                return Err(anyhow!(
                    "Expected duration days: {days}, found {received_days} in {received:?}",
                ));
            }
            if received_total_nanos != total_nanos {
                return Err(anyhow!(
                    "Expected duration with total nanoseconds: {total_nanos}, \
                        found {received_total_nanos} in {received:?}",
                ));
            }
            Ok(())
        }
        _ => Err(anyhow!("Expected duration, found {msg:?}")),
    })))
}

fn build_struct_match_validator(expected: PackStreamStruct) -> ValidateValueFn {
    let expected = PackStreamValue::Struct(expected);
    Box::new(move |msg| {
        if msg == &expected {
            Ok(())
        } else {
            Err(anyhow!("Expected {expected:?} found {msg:?}"))
        }
    })
}

struct ParsedMapKey {
    pub is_optional: bool,
    pub is_ordered: bool,
    pub unescaped: String,
}

impl ParsedMapKey {
    fn parse(key: &str) -> Self {
        thread_local! {
            static UNESCAPE_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(r"\\([\{\}\[\]\\])").unwrap()
            });
        }
        let unescaped = UNESCAPE_RE.with(|re| re.replace_all(key, r"$1"));
        let mut unescaped_ref = unescaped.as_ref();

        thread_local! {
            static FLAGS_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(r"^(\[)?(?:\\[\\\{}\}\[\]]|[^\\])*?(\{\})?(\])?$").unwrap()
            });
        }
        let flags = FLAGS_RE.with(|re| re.captures(key).expect("regex matches anything"));
        let is_optional = flags.get(1).is_some() && flags.get(3).is_some();
        let is_ordered = flags.get(2).is_some();

        if is_optional {
            unescaped_ref = &unescaped_ref[1..unescaped.len() - 1];
        }
        if is_ordered {
            unescaped_ref = &unescaped_ref[..unescaped_ref.len() - 2];
        }

        Self {
            is_optional,
            is_ordered,
            unescaped: unescaped_ref.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remove_optional_marker() {
        let key = "[jeff]";
        let escaped = ParsedMapKey::parse(key);
        assert_eq!(escaped.unescaped, "jeff");
    }

    #[test]
    fn remove_ordered_marker() {
        let key = "jeff{}";
        let escaped = ParsedMapKey::parse(key);
        assert_eq!(escaped.unescaped, "jeff");
    }

    #[test]
    fn remove_both_marker() {
        let key = "[jeff{}]";

        let escaped = ParsedMapKey::parse(key);
        assert_eq!(escaped.unescaped, "jeff");
    }

    #[test]
    fn remove_optional_marker_with_escaped() {
        let key = r"[jeff\{\}]";
        let escaped = ParsedMapKey::parse(key);
        assert_eq!(escaped.unescaped, "jeff{}");
    }

    #[test]
    fn handle_doubled_slash() {
        let key = r"jeff\{}";
        let escaped = ParsedMapKey::parse(key);
        assert_eq!(escaped.unescaped, "jeff{}");
    }
}
