use std::fmt::Debug;

use indexmap::IndexMap;
use serde::ser::SerializeSeq;
use serde::Serialize;
use serde_json::Value as JsonValue;

use super::_parsing::{check_last_json_field, next_json_field, next_pack_stream_field};
use crate::bolt_version::JoltVersion;
use crate::jolt::JoltSigil;
use crate::parse_error::ParseError;
use crate::parser::ActorConfig;
use crate::values::bolt_struct::TAG_UNSUPPORTED_TYPE;
use crate::values::jolt_ser::JoltSigilMapSer;
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

const SIGIL: &str = JoltSigil::UnsupportedType.str();

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct JoltUnsupportedType {
    pub(crate) name: String,
    pub(crate) minimum_protocol_major: i64,
    pub(crate) minimum_protocol_minor: i64,
    pub(crate) message: Option<String>,
    pub(super) jolt_version: JoltVersion,
}

fn supported_jolt_version(jolt_version: JoltVersion) -> bool {
    match jolt_version {
        JoltVersion::V1 | JoltVersion::V2 => false,
        JoltVersion::V3 | JoltVersion::V4 => true,
    }
}

impl JoltUnsupportedType {
    pub(crate) fn parse(
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<Self, ParseError> {
        if !supported_jolt_version(jolt_version) {
            return Err(ParseError::new(format!(
                "Sigil \"{SIGIL}\" (unsupported type) can only be parsed in
                JoltVersion::V3, using {jolt_version:?}"
            )));
        }
        let JsonValue::Array(fields) = v else {
            return Err(ParseError::new(format!(
                "Expected array after sigil \"{SIGIL}\", but found {v:?}"
            )));
        };
        let mut fields = fields.into_iter().enumerate().peekable();
        let i = 0;

        let (i, name) = next_json_field::<String>(&mut fields, "name", i, SIGIL, config)?;
        let (i, minimum_protocol_major) =
            next_json_field::<i64>(&mut fields, "minimum_protocol_major", i, SIGIL, config)?;
        let (i, minimum_protocol_minor) =
            next_json_field::<i64>(&mut fields, "minimum_protocol_minor", i, SIGIL, config)?;
        if fields.peek().is_none() {
            check_last_json_field(&mut fields, i, SIGIL)?;
            return Ok(Self {
                name,
                minimum_protocol_major,
                minimum_protocol_minor,
                message: None,
                jolt_version,
            });
        }
        let (i, message) = next_json_field::<String>(&mut fields, "message", i, SIGIL, config)?;
        check_last_json_field(&mut fields, i, SIGIL)?;
        Ok(Self {
            name,
            minimum_protocol_major,
            minimum_protocol_minor,
            message: Some(message),
            jolt_version,
        })
    }

    pub(crate) fn into_struct(self) -> PackStreamStruct {
        let extra_map = match self.message {
            Some(message) => {
                IndexMap::from([(String::from("message"), PackStreamValue::String(message))])
            }
            None => IndexMap::new(),
        };
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

    pub(super) fn from_struct(s: &PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        if !supported_jolt_version(jolt_version) || s.tag != TAG_UNSUPPORTED_TYPE {
            return None;
        }
        let mut fields = s.fields.iter();
        let name: &str = next_pack_stream_field(&mut fields)?;
        let minimum_protocol_major: i64 = next_pack_stream_field(&mut fields)?;
        let minimum_protocol_minor: i64 = next_pack_stream_field(&mut fields)?;
        let extra: &IndexMap<String, PackStreamValue> = next_pack_stream_field(&mut fields)?;
        let Some(PackStreamValue::String(ref message)) = extra.get("message") else {
            return Some(JoltUnsupportedType {
                name: name.to_string(),
                minimum_protocol_major,
                minimum_protocol_minor,
                message: None,
                jolt_version,
            });
        };
        Some(JoltUnsupportedType {
            name: name.to_string(),
            minimum_protocol_major,
            minimum_protocol_minor,
            message: Some(message.clone()),
            jolt_version,
        })
    }

    pub(super) fn jolt_serialize<S>(
        &self,
        serializer: S,
        jolt_version: JoltVersion,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        struct DataSer<'a>(&'a JoltUnsupportedType);

        impl serde::Serialize for DataSer<'_> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let len = if self.0.message.is_none() { 3 } else { 4 };
                let mut seq = serializer.serialize_seq(Some(len))?;
                seq.serialize_element(&self.0.name)?;
                seq.serialize_element(&self.0.minimum_protocol_major)?;
                seq.serialize_element(&self.0.minimum_protocol_minor)?;
                if let Some(message) = &self.0.message {
                    seq.serialize_element(message)?;
                }
                seq.end()
            }
        }

        let body = DataSer(self);
        let map = JoltSigilMapSer::new(SIGIL, self.jolt_version, jolt_version, &body);
        map.serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;
    use rstest::rstest;
    use rstest_reuse::apply;

    use super::*;

    use crate::bolt_version::{BoltCapabilities, BoltVersion};
    use crate::ext::serde_json::support_pub::JoltSerializer;
    use crate::values::tests::{
        all_jolt_versions, jolt_serializer,
        jolt_versions_v2_and_down as jolt_versions_without_unsupported_type,
        jolt_versions_v3_and_up as jolt_versions_with_unsupported_type,
    };

    fn actor_config() -> ActorConfig {
        ActorConfig {
            bolt_version: BoltVersion::V6_0,
            bolt_version_raw: (6, 0),
            bolt_capabilities: BoltCapabilities::default(),
            handshake_manifest_version: None,
            handshake: None,
            handshake_response: None,
            handshake_delay: None,
            allow_restart: false,
            allow_concurrent: false,
            auto_responses: IndexMap::default(),
            py_lines: Vec::default(),
        }
    }

    #[apply(jolt_versions_with_unsupported_type)]
    #[case::no_message(
        r#"["Quantum Integer", 6, 10]"#,
        JoltUnsupportedType {
            name: "Quantum Integer".to_string(),
            minimum_protocol_major: 6,
            minimum_protocol_minor: 10,
            message: None,
            jolt_version: JoltVersion::V3,
        },
    )]
    #[case::with_message(
        r#"["Quantum Integer", 6, 10, "From the future"]"#,
        JoltUnsupportedType {
            name: "Quantum Integer".to_string(),
            minimum_protocol_major: 6,
            minimum_protocol_minor: 10,
            message: Some("From the future".to_string()),
            jolt_version: JoltVersion::V3,
        },
    )]
    #[case::json_escaping(
        r#"["\"\\", 6, 10, "\\\""]"#,
        JoltUnsupportedType {
            name: "\"\\".to_string(),
            minimum_protocol_major: 6,
            minimum_protocol_minor: 10,
            message: Some("\\\"".to_string()),
            jolt_version: JoltVersion::V3,
        },
    )]
    fn test_jolt_unsupported_type_parse(
        #[case] input: &str,
        #[case] mut expected: JoltUnsupportedType,
        jolt_version: JoltVersion,
    ) {
        expected.jolt_version = jolt_version;
        let json = serde_json::from_str(input).unwrap();
        let jolt_version = JoltVersion::V3;
        expected.jolt_version = jolt_version;
        let config = actor_config();

        let result = JoltUnsupportedType::parse(json, jolt_version, &config).unwrap();

        assert_eq!(result.name, expected.name);
        assert_eq!(
            result.minimum_protocol_major,
            expected.minimum_protocol_major
        );
        assert_eq!(
            result.minimum_protocol_minor,
            expected.minimum_protocol_minor
        );
        assert_eq!(result.message, expected.message);
    }

    #[rstest]
    #[case::wrong_type(r"1")]
    #[case::array_too_short(r#"["quantum integer", 6]"#)]
    #[case::array_too_long(r#"["quantum integer", 6, 10, "future", "from the"]"#)]
    #[case::invalid_data(r#"[1, 6, 10, "future"]"#)]
    #[case::invalid_data(r#"["quantum integer", "6", 10, "future"]"#)]
    #[case::invalid_data(r#"["quantum integer", 6, "10", "future"]"#)]
    #[case::invalid_data(r#"["quantum integer", 6, 10, 11]"#)]
    fn test_jolt_unsupported_type_parse_invalid(#[case] input: &str) {
        let json = serde_json::from_str(input).unwrap();
        let jolt_version = JoltVersion::V3;
        let config = actor_config();

        JoltUnsupportedType::parse(json, jolt_version, &config).unwrap_err();
    }

    #[apply(jolt_versions_without_unsupported_type)]
    fn test_jolt_unsupported_type_parse_invalid_jolt_version(jolt_version: JoltVersion) {
        let json = serde_json::from_str(r#"["Quantum Integer", 6, 10]"#).unwrap();
        let config = actor_config();

        JoltUnsupportedType::parse(json, jolt_version, &config).unwrap_err();
    }

    #[apply(all_jolt_versions)]
    #[case::no_message_v3(
        JoltUnsupportedType {
            name: "Quantum Integer".to_string(),
            minimum_protocol_major: 6,
            minimum_protocol_minor: 10,
            message: None,
            jolt_version: JoltVersion::V3,
        },
        r#"{"UT": ["Quantum Integer", 6, 10]}"#,
        r#"{"UTv3": ["Quantum Integer", 6, 10]}"#,
    )]
    #[case::with_message_v3(
        JoltUnsupportedType {
            name: "Quantum Integer".to_string(),
            minimum_protocol_major: 6,
            minimum_protocol_minor: 10,
            message: Some("From the future".to_string()),
            jolt_version: JoltVersion::V3,
        },
        r#"{"UT": ["Quantum Integer", 6, 10, "From the future"]}"#,
        r#"{"UTv3": ["Quantum Integer", 6, 10, "From the future"]}"#,
    )]
    #[case::no_message_v4(
        JoltUnsupportedType {
            name: "Quantum Integer".to_string(),
            minimum_protocol_major: 6,
            minimum_protocol_minor: 10,
            message: None,
            jolt_version: JoltVersion::V4,
        },
        r#"{"UT": ["Quantum Integer", 6, 10]}"#,
        r#"{"UTv4": ["Quantum Integer", 6, 10]}"#,
    )]
    #[case::with_message_v4(
        JoltUnsupportedType {
            name: "Quantum Integer".to_string(),
            minimum_protocol_major: 6,
            minimum_protocol_minor: 10,
            message: Some("From the future".to_string()),
            jolt_version: JoltVersion::V4,
        },
        r#"{"UT": ["Quantum Integer", 6, 10, "From the future"]}"#,
        r#"{"UTv4": ["Quantum Integer", 6, 10, "From the future"]}"#,
    )]
    fn test_jolt_fmt(
        #[case] jolt_unsupported_type: JoltUnsupportedType,
        #[case] expected: &str,
        #[case] expected_versioned: &str,
        jolt_version: JoltVersion,
        mut jolt_serializer: JoltSerializer<Vec<u8>>,
    ) {
        jolt_unsupported_type
            .jolt_serialize(jolt_serializer.ser(), jolt_version)
            .expect("Failed to serialize Jolt");
        let formatted = jolt_serializer.into_string();
        if jolt_version == jolt_unsupported_type.jolt_version {
            assert_eq!(formatted, expected);
        } else {
            assert_eq!(formatted, expected_versioned);
        }
    }

    #[rstest]
    #[case(None)]
    #[case(Some(String::from("From the future")))]
    fn test_struct_parsing(#[case] message: Option<String>) {
        let extra = match message.clone() {
            Some(message) => IndexMap::from([(
                String::from("message"),
                PackStreamValue::String(message.clone()),
            )]),
            None => IndexMap::new(),
        };
        let structure = PackStreamStruct {
            tag: TAG_UNSUPPORTED_TYPE,
            fields: vec![
                PackStreamValue::String(String::from("Quantum Integer")),
                PackStreamValue::Integer(6),
                PackStreamValue::Integer(10),
                PackStreamValue::Dict(extra),
            ],
        };
        assert_eq!(
            JoltUnsupportedType::from_struct(&structure, JoltVersion::V3),
            Some(JoltUnsupportedType {
                name: String::from("Quantum Integer"),
                minimum_protocol_major: 6,
                minimum_protocol_minor: 10,
                message,
                jolt_version: JoltVersion::V3,
            })
        );
    }
}
