use std::fmt::Debug;

use indexmap::IndexMap;
use serde_json::Value as JsonValue;

use crate::parse_error::ParseError;
use crate::parser::{transcode_field, ActorConfig};
use crate::values::pack_stream_value::PackStreamValue;

#[allow(private_bounds)]
pub(in super::super) fn next_json_field<R: ExtractableField>(
    fields: &mut impl Iterator<Item = (usize, JsonValue)>,
    name: &str,
    field_num: usize,
    sigil: &str,
    config: &ActorConfig,
) -> Result<(usize, R), ParseError> {
    let Some((field_num, field)) = fields.next() else {
        return Err(ParseError::new(format!(
            "Missing array entry {name} (field {}) after sigil \"{sigil}\"",
            field_num + 1
        )));
    };
    let field = match R::extract(field, config) {
        Ok(field) => field,
        Err(err) => return Err(err.into_parse_error(name, field_num, sigil)),
    };
    Ok((field_num, field))
}

pub(in super::super) fn check_last_json_field(
    fields: &mut impl Iterator<Item = (usize, JsonValue)>,
    field_num: usize,
    sigil: &str,
) -> Result<(), ParseError> {
    if fields.next().is_some() {
        return Err(ParseError::new(format!(
            "Too many fields after sigil \"{sigil}\", expected {}",
            field_num + 1
        )));
    };
    Ok(())
}

#[derive(Debug)]
enum ExtractionFailure {
    Expectation(String),
    Nested(ParseError),
}

impl ExtractionFailure {
    fn into_parse_error(self, name: &str, field_num: usize, sigil: &str) -> ParseError {
        match self {
            Self::Expectation(expectation) => ParseError::new(format!(
                "Expected {name} (field {field_num}) after sigil \"{sigil}\" {expectation}"
            )),
            Self::Nested(err) => err,
        }
    }
}

impl From<String> for ExtractionFailure {
    fn from(expectation: String) -> Self {
        Self::Expectation(expectation)
    }
}

impl From<ParseError> for ExtractionFailure {
    fn from(err: ParseError) -> Self {
        Self::Nested(err)
    }
}

trait ExtractableField: Sized {
    fn extract(field: JsonValue, config: &ActorConfig) -> Result<Self, ExtractionFailure>;
}

impl ExtractableField for i64 {
    fn extract(field: JsonValue, _: &ActorConfig) -> Result<Self, ExtractionFailure> {
        field
            .as_i64()
            .ok_or_else(|| format!("to be i64, but found {field:?}").into())
    }
}

impl ExtractableField for Vec<String> {
    fn extract(field: JsonValue, _: &ActorConfig) -> Result<Self, ExtractionFailure> {
        let JsonValue::Array(field) = field else {
            return Err(format!("to be an array, but found {field:?}").into());
        };
        field
            .into_iter()
            .enumerate()
            .map(|(i, l)| {
                let JsonValue::String(l) = l else {
                    return Err(format!(
                        "to be an array of strings, but found {l:?} (at {})",
                        i + 1,
                    )
                    .into());
                };
                Ok(l)
            })
            .collect()
    }
}

impl ExtractableField for IndexMap<String, PackStreamValue> {
    fn extract(field: JsonValue, config: &ActorConfig) -> Result<Self, ExtractionFailure> {
        let JsonValue::Object(field) = field else {
            return Err(format!("to be a map, but found {field:?}").into());
        };
        field
            .into_iter()
            .map(|(k, v)| transcode_field(v, config).map(|v| (k, v)))
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }
}

impl ExtractableField for String {
    fn extract(field: JsonValue, _: &ActorConfig) -> Result<Self, ExtractionFailure> {
        let JsonValue::String(field) = field else {
            return Err(format!("to be string, but found {field:?}").into());
        };
        Ok(field)
    }
}
