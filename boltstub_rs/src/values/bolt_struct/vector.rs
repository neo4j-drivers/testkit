use std::fmt::Debug;

use serde::Serialize;
use serde_json::Value as JsonValue;

use super::_parsing::{
    check_last_json_field, check_last_pack_stream_field, next_json_field, next_pack_stream_field,
};
use crate::bolt_version::JoltVersion;
use crate::jolt::JoltSigil;
use crate::parse_error::ParseError;
use crate::parser::ActorConfig;
use crate::str_bytes::{parse_jolt_hex_string, serialize_fmt_bytes};
use crate::values::bolt_struct::TAG_VECTOR;
use crate::values::jolt_ser::JoltSigilMapSer;
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

const SIGIL: &str = JoltSigil::Vector.str();

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JoltVectorType {
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
}

impl JoltVectorType {
    pub(crate) fn from_jolt(s: &str) -> Option<Self> {
        Some(match s {
            "i8" => JoltVectorType::Int8,
            "i16" => JoltVectorType::Int16,
            "i32" => JoltVectorType::Int32,
            "i64" => JoltVectorType::Int64,
            "f32" => JoltVectorType::Float32,
            "f64" => JoltVectorType::Float64,
            _ => return None,
        })
    }

    pub(crate) fn to_jolt(&self) -> &'static str {
        match self {
            JoltVectorType::Int8 => "i8",
            JoltVectorType::Int16 => "i16",
            JoltVectorType::Int32 => "i32",
            JoltVectorType::Int64 => "i64",
            JoltVectorType::Float32 => "f32",
            JoltVectorType::Float64 => "f64",
        }
    }

    pub(crate) fn packstream_marker(&self) -> u8 {
        match self {
            JoltVectorType::Int8 => 0xC8,
            JoltVectorType::Int16 => 0xC9,
            JoltVectorType::Int32 => 0xCA,
            JoltVectorType::Int64 => 0xCB,
            JoltVectorType::Float32 => 0xC6,
            JoltVectorType::Float64 => 0xC1,
        }
    }

    pub(crate) fn from_packstream_marker(marker: u8) -> Option<Self> {
        Some(match marker {
            0xC8 => JoltVectorType::Int8,
            0xC9 => JoltVectorType::Int16,
            0xCA => JoltVectorType::Int32,
            0xCB => JoltVectorType::Int64,
            0xC6 => JoltVectorType::Float32,
            0xC1 => JoltVectorType::Float64,
            _ => return None,
        })
    }
}

impl JoltVectorType {
    pub(crate) fn size(&self) -> usize {
        match self {
            JoltVectorType::Int8 => 1,
            JoltVectorType::Int16 => 2,
            JoltVectorType::Int32 => 4,
            JoltVectorType::Int64 => 8,
            JoltVectorType::Float32 => 4,
            JoltVectorType::Float64 => 8,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct JoltVector {
    pub(crate) inner_type: JoltVectorType,
    pub(crate) data: Vec<u8>,
    pub(super) jolt_version: JoltVersion,
}

fn supported_jolt_versions(jolt_version: JoltVersion) -> bool {
    match jolt_version {
        JoltVersion::V1 | JoltVersion::V2 => false,
        JoltVersion::V3 | JoltVersion::V4 => true,
    }
}

impl JoltVector {
    pub(crate) fn parse(
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<Self, ParseError> {
        if !supported_jolt_versions(jolt_version) {
            return Err(ParseError::new(format!(
                "Sigil \"{SIGIL}\" (vector) can only be parsed in \
                JoltVersion::V3+, using {jolt_version:?}"
            )));
        }
        let JsonValue::Array(fields) = v else {
            return Err(ParseError::new(format!(
                "Expected array after sigil \"{SIGIL}\", but found {v:?}"
            )));
        };
        let mut fields = fields.into_iter().enumerate();
        let i = 0;

        let (i, inner_type) =
            next_json_field::<String>(&mut fields, "inner type", i, SIGIL, config)?;
        let inner_type = JoltVectorType::from_jolt(&inner_type).ok_or_else(|| {
            ParseError::new(format!(
                "Failed to parse inner type (field 1) after sigil \"{SIGIL}\": \
                Unknown inner vector type: {inner_type}"
            ))
        })?;
        let (i, raw_data) = next_json_field::<String>(&mut fields, "raw data", i, SIGIL, config)?;
        let data = parse_jolt_hex_string(&raw_data).map_err(|e| {
            ParseError::new(format!(
                "Failed to parse raw data (field 2) after sigil \"{SIGIL}\": {e}"
            ))
        })?;
        if data.len() % inner_type.size() != 0 {
            return Err(ParseError::new(format!(
                "Raw data (field 2) after sigil \"{SIGIL}\" \
                is not a multiple of the inner type size: {}",
                inner_type.size()
            )));
        }
        check_last_json_field(&mut fields, i, SIGIL)?;

        Ok(Self {
            inner_type,
            data,
            jolt_version,
        })
    }

    pub(crate) fn into_struct(self) -> PackStreamStruct {
        let fields = vec![
            PackStreamValue::Bytes(vec![self.inner_type.packstream_marker()]),
            PackStreamValue::Bytes(self.data),
        ];
        PackStreamStruct {
            tag: TAG_VECTOR,
            fields,
        }
    }

    pub(super) fn from_struct(s: &PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        if !supported_jolt_versions(jolt_version) || s.tag != TAG_VECTOR {
            return None;
        }
        let mut fields = s.fields.iter();
        let type_marker: &[u8] = next_pack_stream_field(&mut fields)?;
        let [type_marker] = *type_marker else {
            return None;
        };
        let inner_type = JoltVectorType::from_packstream_marker(type_marker)?;
        let data: &[u8] = next_pack_stream_field(&mut fields)?;
        if !check_last_pack_stream_field(&mut fields) {
            return None;
        }

        let data = data.to_vec();
        Some(JoltVector {
            inner_type,
            data,
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
        let body = (self.inner_type.to_jolt(), serialize_fmt_bytes(&self.data));
        let map = JoltSigilMapSer::new(SIGIL, self.jolt_version, jolt_version, &body);
        map.serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;
    use rstest::rstest;
    use rstest_reuse::apply;

    use crate::bolt_version::{BoltCapabilities, BoltVersion};
    use crate::ext::serde_json::support_pub::JoltSerializer;
    use crate::values::tests::{
        all_jolt_versions, jolt_serializer,
        jolt_versions_v2_and_down as jolt_versions_without_vector,
        jolt_versions_v3_and_up as jolt_versions_with_vector,
    };

    use super::*;

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

    #[apply(jolt_versions_with_vector)]
    #[case::empty_i8(r#"["i8", ""]"#, JoltVector {
        inner_type: JoltVectorType::Int8,
        data: vec![],
        jolt_version: JoltVersion::V3,
    })]
    #[case::empty_i16(r#"["i16", ""]"#, JoltVector {
        inner_type: JoltVectorType::Int16,
        data: vec![],
        jolt_version: JoltVersion::V3,
    })]
    #[case::empty_i32(r#"["i32", ""]"#, JoltVector {
        inner_type: JoltVectorType::Int32,
        data: vec![],
        jolt_version: JoltVersion::V3,
    })]
    #[case::empty_i64(r#"["i64", ""]"#, JoltVector {
        inner_type: JoltVectorType::Int64,
        data: vec![],
        jolt_version: JoltVersion::V3,
    })]
    #[case::empty_f32(r#"["f32", ""]"#, JoltVector {
        inner_type: JoltVectorType::Float32,
        data: vec![],
        jolt_version: JoltVersion::V3,
    })]
    #[case::empty_f64(r#"["f64", ""]"#, JoltVector {
        inner_type: JoltVectorType::Float64,
        data: vec![],
        jolt_version: JoltVersion::V3,
    })]
    #[case::data_i8(r#"["i8", "0F"]"#, JoltVector {
        inner_type: JoltVectorType::Int8,
        data: vec![0x0F],
        jolt_version: JoltVersion::V3,
    })]
    #[case::data_i16(r#"["i16", "0F 1F"]"#, JoltVector {
        inner_type: JoltVectorType::Int16,
        data: vec![0x0F, 0x1F],
        jolt_version: JoltVersion::V3,
    })]
    #[case::data_i32(r#"["i32", "0F 1F 2F 3F"]"#, JoltVector {
        inner_type: JoltVectorType::Int32,
        data: vec![0x0F, 0x1F, 0x2F, 0x3F],
        jolt_version: JoltVersion::V3,
    })]
    #[case::data_i64(r#"["i64", "0F 1F 2F 3F 4F 5F 6F 7F"]"#, JoltVector {
        inner_type: JoltVectorType::Int64,
        data: vec![0x0F, 0x1F, 0x2F, 0x3F, 0x4F, 0x5F, 0x6F, 0x7F],
        jolt_version: JoltVersion::V3,
    })]
    #[case::data_f32(r#"["f32", "0F 1F 2F 3F"]"#, JoltVector {
        inner_type: JoltVectorType::Float32,
        data: vec![0x0F, 0x1F, 0x2F, 0x3F],
        jolt_version: JoltVersion::V3,
    })]
    #[case::data_f64(r#"["f64", "0F 1F 2F 3F 4F 5F 6F 7F"]"#, JoltVector {
        inner_type: JoltVectorType::Float64,
        data: vec![0x0F, 0x1F, 0x2F, 0x3F, 0x4F, 0x5F, 0x6F, 0x7F],
        jolt_version: JoltVersion::V3,
    })]
    fn test_jolt_vector_parse(
        #[case] input: &str,
        #[case] mut expected: JoltVector,
        jolt_version: JoltVersion,
    ) {
        expected.jolt_version = jolt_version;
        let json = serde_json::from_str(input).unwrap();
        let config = actor_config();

        let result = JoltVector::parse(json, expected.jolt_version, &config).unwrap();

        assert_eq!(result.inner_type, expected.inner_type);
        assert_eq!(result.data, expected.data);
    }

    #[rstest]
    #[case::wrong_type("1")]
    #[case::array_too_short(r#"["i8"]"#)]
    #[case::array_too_long(r#"["i8", "", ""]"#)]
    #[case::wrong_inner_type_type(r#"[1, ""]"#)]
    #[case::unknown_inner_type(r#"["f16", ""]"#)]
    #[case::unknown_inner_type(r#"["f128", ""]"#)]
    #[case::unknown_inner_type(r#"["u64", ""]"#)]
    #[case::unknown_inner_type(r#"["i128", ""]"#)]
    #[case::unknown_inner_type(r#"["int", ""]"#)]
    #[case::unknown_inner_type(r#"["foobar", ""]"#)]
    #[case::invalid_data(r#"["i8", "0x12"]"#)]
    #[case::invalid_data(r#"["i8", [1, 255]]"#)]
    #[case::wrong_data_len(r#"["i16", "FF"]"#)]
    #[case::wrong_data_len(r#"["i32", "FF FF FF"]"#)]
    #[case::wrong_data_len(r#"["i64", "FF FF FF FF FF FF FF"]"#)]
    #[case::wrong_data_len(r#"["f32", "FF FF FF"]"#)]
    #[case::wrong_data_len(r#"["f64", "FF FF FF FF FF FF FF"]"#)]
    fn test_jolt_vector_parse_invalid(#[case] input: &str) {
        let json = serde_json::from_str(input).unwrap();
        let jolt_version = JoltVersion::V3;
        let config = actor_config();

        JoltVector::parse(json, jolt_version, &config).unwrap_err();
    }

    #[apply(jolt_versions_without_vector)]
    fn test_jolt_vector_parse_invalid_jolt_version(jolt_version: JoltVersion) {
        let json = serde_json::from_str(r#"["i8", ""]"#).unwrap();
        let config = actor_config();

        JoltVector::parse(json, jolt_version, &config).unwrap_err();
    }

    #[apply(all_jolt_versions)]
    #[case::empty_i8(
        JoltVector {
            inner_type: JoltVectorType::Int8,
            data: vec![],
            jolt_version: JoltVersion::V3,
        },
        r#"{"V": ["i8", ""]}"#,
        r#"{"Vv3": ["i8", ""]}"#,
    )]
    #[case::empty_i16(
        JoltVector {
            inner_type: JoltVectorType::Int16,
            data: vec![],
            jolt_version: JoltVersion::V3,
        },
        r#"{"V": ["i16", ""]}"#,
        r#"{"Vv3": ["i16", ""]}"#,
    )]
    #[case::empty_i32(
        JoltVector {
            inner_type: JoltVectorType::Int32,
            data: vec![],
            jolt_version: JoltVersion::V3,
        },
        r#"{"V": ["i32", ""]}"#,
        r#"{"Vv3": ["i32", ""]}"#,
    )]
    #[case::empty_i64(
        JoltVector {
            inner_type: JoltVectorType::Int64,
            data: vec![],
            jolt_version: JoltVersion::V3,
        },
        r#"{"V": ["i64", ""]}"#,
        r#"{"Vv3": ["i64", ""]}"#,
    )]
    #[case::empty_f32(
        JoltVector {
            inner_type: JoltVectorType::Float32,
            data: vec![],
            jolt_version: JoltVersion::V3,
        },
        r#"{"V": ["f32", ""]}"#,
        r#"{"Vv3": ["f32", ""]}"#,
    )]
    #[case::empty_f64(
        JoltVector {
            inner_type: JoltVectorType::Float64,
            data: vec![],
            jolt_version: JoltVersion::V3,
        },
        r#"{"V": ["f64", ""]}"#,
        r#"{"Vv3": ["f64", ""]}"#,
    )]
    #[case::data_i8(
        JoltVector {
            inner_type: JoltVectorType::Int8,
            data: vec![0x0F],
            jolt_version: JoltVersion::V3,
        },
        r#"{"V": ["i8", "0F"]}"#,
        r#"{"Vv3": ["i8", "0F"]}"#,
    )]
    #[case::data_i16(
        JoltVector {
            inner_type: JoltVectorType::Int16,
            data: vec![0x0F, 0x1F],
            jolt_version: JoltVersion::V3,
        },
        r#"{"V": ["i16", "0F 1F"]}"#,
        r#"{"Vv3": ["i16", "0F 1F"]}"#,
    )]
    #[case::data_i32(
        JoltVector {
            inner_type: JoltVectorType::Int32,
            data: vec![0x0F, 0x1F, 0x2F, 0x3F],
            jolt_version: JoltVersion::V3,
        },
        r#"{"V": ["i32", "0F 1F 2F 3F"]}"#,
        r#"{"Vv3": ["i32", "0F 1F 2F 3F"]}"#,
    )]
    #[case::data_i64(
        JoltVector {
            inner_type: JoltVectorType::Int64,
            data: vec![0x0F, 0x1F, 0x2F, 0x3F, 0x4F, 0x5F, 0x6F, 0x7F],
            jolt_version: JoltVersion::V3,
        },
        r#"{"V": ["i64", "0F 1F 2F 3F 4F 5F 6F 7F"]}"#,
        r#"{"Vv3": ["i64", "0F 1F 2F 3F 4F 5F 6F 7F"]}"#,
    )]
    #[case::data_f32(
        JoltVector {
            inner_type: JoltVectorType::Float32,
            data: vec![0x0F, 0x1F, 0x2F, 0x3F],
            jolt_version: JoltVersion::V3,
        },
        r#"{"V": ["f32", "0F 1F 2F 3F"]}"#,
        r#"{"Vv3": ["f32", "0F 1F 2F 3F"]}"#,
    )]
    #[case::data_f64(
        JoltVector {
            inner_type: JoltVectorType::Float64,
            data: vec![0x0F, 0x1F, 0x2F, 0x3F, 0x4F, 0x5F, 0x6F, 0x7F],
            jolt_version: JoltVersion::V3,
        },
        r#"{"V": ["f64", "0F 1F 2F 3F 4F 5F 6F 7F"]}"#,
        r#"{"Vv3": ["f64", "0F 1F 2F 3F 4F 5F 6F 7F"]}"#,
    )]
    #[case::data_f64_v4(
        JoltVector {
            inner_type: JoltVectorType::Float64,
            data: vec![0x0F, 0x1F, 0x2F, 0x3F, 0x4F, 0x5F, 0x6F, 0x7F],
            jolt_version: JoltVersion::V4,
        },
        r#"{"V": ["f64", "0F 1F 2F 3F 4F 5F 6F 7F"]}"#,
        r#"{"Vv4": ["f64", "0F 1F 2F 3F 4F 5F 6F 7F"]}"#,
    )]
    fn test_jolt_fmt(
        #[case] jolt_vector: JoltVector,
        #[case] expected: &str,
        #[case] expected_versioned: &str,
        jolt_version: JoltVersion,
        mut jolt_serializer: JoltSerializer<Vec<u8>>,
    ) {
        jolt_vector
            .jolt_serialize(jolt_serializer.ser(), jolt_version)
            .expect("Failed to serialize Jolt");
        let formatted = jolt_serializer.into_string();

        if jolt_vector.jolt_version == jolt_version {
            assert_eq!(formatted, expected);
        } else {
            assert_eq!(formatted, expected_versioned);
        }
    }
}
