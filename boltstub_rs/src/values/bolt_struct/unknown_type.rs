use std::fmt::{Debug, Display, Formatter};

use serde_json::Value as JsonValue;

use super::_parsing::{check_last_json_field, next_json_field, next_pack_stream_field};
use crate::bolt_version::JoltVersion;
use crate::parse_error::ParseError;
use crate::parser::ActorConfig;
use crate::values::bolt_struct::TAG_UNKNOWN_TYPE;
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};
use indexmap::IndexMap;

#[derive(Debug, Clone)]
pub(crate) struct JoltUnknownType {
    pub(crate) name: String,
    pub(crate) min_bolt: String,
    pub(crate) message: String,
}

impl JoltUnknownType {
    pub(crate) fn parse(
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<Self, ParseError> {
        if !matches!(jolt_version, JoltVersion::V3) {
            return Err(ParseError::new(format!(
                "Sigil \"W\" (unknown type) can only be parsed in JoltVersion::V3, using {jolt_version:?}"
            )));
        }
        let JsonValue::Array(fields) = v else {
            return Err(ParseError::new(
                "Expected array after sigil \"W\", but found {v:?}",
            ));
        };
        let mut fields = fields.into_iter().enumerate();
        let i = 0;

        let (i, name) = next_json_field::<String>(&mut fields, "name", i, "W", config)?;
        let (i, min_bolt) = next_json_field::<String>(&mut fields, "min_bolt", i, "W", config)?;
        let (i, extra) = next_json_field::<IndexMap<String, PackStreamValue>>(
            &mut fields,
            "extra",
            i,
            "W",
            config,
        )?;
        check_last_json_field(&mut fields, i, "W")?;
        let PackStreamValue::String(ref message) = extra["message"] else {
            return Err(ParseError::new("Expected message in extra to be string."));
        };
        Ok(Self {
            name,
            min_bolt,
            message: message.clone(),
        })
    }

    pub(crate) fn into_struct(self) -> PackStreamStruct {
        let mut extra_map = IndexMap::with_capacity(1);
        extra_map.insert(
            String::from("message"),
            PackStreamValue::String(self.message),
        );
        let fields = vec![
            PackStreamValue::String(self.name),
            PackStreamValue::String(self.min_bolt),
            PackStreamValue::Dict(extra_map),
        ];
        PackStreamStruct {
            tag: TAG_UNKNOWN_TYPE,
            fields,
        }
    }
}
#[derive(Debug, Clone)]
pub(super) struct BoltUnknownType(pub(super) JoltUnknownType);
impl BoltUnknownType {
    pub(super) fn from_struct(s: &PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        if !matches!(jolt_version, JoltVersion::V3) || s.tag != TAG_UNKNOWN_TYPE {
            return None;
        }
        let mut fields = s.fields.iter();
        let name: &str = next_pack_stream_field(&mut fields)?;
        let min_bolt: &str = next_pack_stream_field(&mut fields)?;
        let extra: &IndexMap<String, PackStreamValue> = next_pack_stream_field(&mut fields)?;
        let PackStreamValue::String(ref message) = extra["message"] else {
            return None;
        };
        Some(Self(JoltUnknownType {
            name: name.to_string(),
            min_bolt: min_bolt.to_string(),
            message: message.clone(),
        }))
    }

    pub(super) fn jolt_fmt(&self, _jolt_version: JoltVersion) -> impl Display + '_ {
        struct JoltFormatter<'a> {
            this: &'a JoltUnknownType,
        }

        impl Display for JoltFormatter<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.write_str(r#"{"W": [""#)?;
                f.write_str(&self.this.name)?;

                f.write_str(r#"", ""#)?;
                f.write_str(&self.this.min_bolt)?;

                f.write_str(r#"", {message: ""#)?;
                f.write_str(&self.this.message)?;
                f.write_str(r#""}]}"#)
            }
        }

        JoltFormatter { this: &self.0 }
    }
}
