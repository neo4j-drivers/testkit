use std::fmt::{Debug, Display, Formatter};

use indexmap::IndexMap;
use serde_json::Value as JsonValue;

use super::_parsing::{check_last_json_field, next_json_field, next_pack_stream_field};
use crate::bolt_version::JoltVersion;
use crate::parse_error::ParseError;
use crate::parser::ActorConfig;
use crate::values::bolt_struct::TAG_UNSUPPORTED_TYPE;
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

#[derive(Debug, Clone)]
pub(crate) struct JoltUnsupportedType {
    pub(crate) name: String,
    pub(crate) minimum_protocol_major: i64,
    pub(crate) minimum_protocol_minor: i64,
    pub(crate) message: Option<String>,
}

impl JoltUnsupportedType {
    pub(crate) fn parse(
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<Self, ParseError> {
        match jolt_version {
            JoltVersion::V1 | JoltVersion::V2 => {
                return Err(ParseError::new(format!(
                    "Sigil \"UT\" (unknown type) can only be parsed in JoltVersion::V3,
                    using {jolt_version:?}"
                )));
            }
            JoltVersion::V3 => {}
        }
        let JsonValue::Array(fields) = v else {
            return Err(ParseError::new(
                "Expected array after sigil \"UT\", but found {v:?}",
            ));
        };
        let mut fields = fields.into_iter().enumerate().peekable();
        let i = 0;

        let (i, name) = next_json_field::<String>(&mut fields, "name", i, "UT", config)?;
        let (i, minimum_protocol_major) =
            next_json_field::<i64>(&mut fields, "minimum_protocol_major", i, "UT", config)?;
        let (i, minimum_protocol_minor) =
            next_json_field::<i64>(&mut fields, "minimum_protocol_minor", i, "UT", config)?;
        if fields.peek().is_none() {
            check_last_json_field(&mut fields, i, "UT")?;
            return Ok(Self {
                name,
                minimum_protocol_major,
                minimum_protocol_minor,
                message: None,
            });
        }
        let (i, message) = next_json_field::<String>(&mut fields, "message", i, "UT", config)?;
        check_last_json_field(&mut fields, i, "UT")?;
        Ok(Self {
            name,
            minimum_protocol_major,
            minimum_protocol_minor,
            message: Some(message),
        })
    }

    pub(crate) fn into_struct(self) -> PackStreamStruct {
        let mut extra_map = IndexMap::with_capacity(1);
        if let Some(message) = self.message {
            extra_map.insert(String::from("message"), PackStreamValue::String(message));
        }
        let fields = vec![
            PackStreamValue::String(self.name),
            PackStreamValue::Integer(self.minimum_protocol_major),
            PackStreamValue::Integer(self.minimum_protocol_minor),
            PackStreamValue::Dict(extra_map),
        ];
        PackStreamStruct {
            tag: TAG_UNSUPPORTED_TYPE,
            fields,
        }
    }
}
#[derive(Debug, Clone)]
pub(super) struct BoltUnsupportedType(pub(super) JoltUnsupportedType);
impl BoltUnsupportedType {
    pub(super) fn from_struct(s: &PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        match jolt_version {
            JoltVersion::V1 | JoltVersion::V2 => return None,
            JoltVersion::V3 => {}
        }
        let mut fields = s.fields.iter();
        let name: &str = next_pack_stream_field(&mut fields)?;
        let minimum_protocol_major: i64 = next_pack_stream_field(&mut fields)?;
        let minimum_protocol_minor: i64 = next_pack_stream_field(&mut fields)?;
        let extra: &IndexMap<String, PackStreamValue> = next_pack_stream_field(&mut fields)?;
        let PackStreamValue::String(ref message) = extra["message"] else {
            return Some(Self(JoltUnsupportedType {
                name: name.to_string(),
                minimum_protocol_major,
                minimum_protocol_minor,
                message: None,
            }));
        };
        Some(Self(JoltUnsupportedType {
            name: name.to_string(),
            minimum_protocol_major,
            minimum_protocol_minor,
            message: Some(message.clone()),
        }))
    }

    pub(super) fn jolt_fmt(&self, _jolt_version: JoltVersion) -> impl Display + '_ {
        struct JoltFormatter<'a> {
            this: &'a JoltUnsupportedType,
        }

        impl Display for JoltFormatter<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.write_str(r#"{"UT": [""#)?;
                f.write_str(&self.this.name)?;

                f.write_str(r#"", ""#)?;
                Display::fmt(&self.this.minimum_protocol_major, f)?;

                f.write_str(r#"", ""#)?;
                Display::fmt(&self.this.minimum_protocol_minor, f)?;
                if self.this.message.is_none() {
                    f.write_str(r#"", ""#)?;
                    f.write_str(&self.this.message.clone().expect("TEST"))?;
                }
                f.write_str(r#""]}"#)
            }
        }

        JoltFormatter { this: &self.0 }
    }
}
