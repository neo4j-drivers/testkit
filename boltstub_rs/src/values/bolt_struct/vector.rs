use std::fmt::{Debug, Display, Formatter};

use serde_json::Value as JsonValue;

use super::_parsing::{
    check_last_json_field, check_last_pack_stream_field, next_json_field, next_pack_stream_field,
};
use crate::bolt_version::JoltVersion;
use crate::parse_error::ParseError;
use crate::parser::ActorConfig;
use crate::str_bytes::{fmt_bytes, parse_jolt_hex_string};
use crate::values::bolt_struct::TAG_VECTOR;
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

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
}

impl JoltVector {
    pub(crate) fn parse(
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<Self, ParseError> {
        if !matches!(jolt_version, JoltVersion::V3) {
            return Err(ParseError::new(format!(
                "Sigil \"T\" (vector) can only be parsed in JoltVersion::V3, using {jolt_version:?}"
            )));
        }
        let JsonValue::Array(fields) = v else {
            return Err(ParseError::new(format!(
                "Expected array after sigil \"V\", but found {v:?}"
            )));
        };
        let mut fields = fields.into_iter().enumerate();
        let i = 0;

        let (i, inner_type) = next_json_field::<String>(&mut fields, "inner type", i, "V", config)?;
        let inner_type = JoltVectorType::from_jolt(&inner_type).ok_or_else(|| {
            ParseError::new(format!(
                "Failed to parse inner type (field 1) after sigil \"V\": \
                Unknown inner vector type: {inner_type}"
            ))
        })?;
        let (i, raw_data) = next_json_field::<String>(&mut fields, "raw data", i, "V", config)?;
        let data = parse_jolt_hex_string(&raw_data).map_err(|e| {
            ParseError::new(format!(
                "Failed to parse raw data (field 2) after sigil \"V\": {e}"
            ))
        })?;
        if data.len() % inner_type.size() != 0 {
            return Err(ParseError::new(format!(
                "Raw data (field 2) after sigil \"V\" is not a multiple of the inner type size: {}",
                inner_type.size()
            )));
        }
        check_last_json_field(&mut fields, i, "V")?;

        Ok(Self { inner_type, data })
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
}
#[derive(Debug, Clone)]
pub(super) struct BoltVector(pub(super) JoltVector);
impl BoltVector {
    pub(super) fn from_struct(s: &PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        if !matches!(jolt_version, JoltVersion::V3) || s.tag != TAG_VECTOR {
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
        Some(Self(JoltVector { inner_type, data }))
    }

    pub(super) fn jolt_fmt(&self, _jolt_version: JoltVersion) -> impl Display + '_ {
        struct JoltFormatter<'a> {
            this: &'a JoltVector,
        }

        impl Display for JoltFormatter<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.write_str(r#"{"V": [""#)?;
                Display::fmt(self.this.inner_type.to_jolt(), f)?;

                f.write_str(r#"", ""#)?;
                fmt_bytes(&self.this.data).fmt(f)?;

                f.write_str(r#""]}"#)
            }
        }

        JoltFormatter { this: &self.0 }
    }
}

#[cfg(test)]
mod test {
    use crate::bolt_version::BoltVersion;

    use rstest::rstest;

    use super::*;

    fn actor_config() -> ActorConfig {
        ActorConfig {
            bolt_version: BoltVersion::V6_0,
            bolt_version_raw: (6, 0),
            bolt_capabilities: Default::default(),
            handshake_manifest_version: Default::default(),
            handshake: Default::default(),
            handshake_response: Default::default(),
            handshake_delay: Default::default(),
            allow_restart: Default::default(),
            allow_concurrent: Default::default(),
            auto_responses: Default::default(),
            py_lines: Default::default(),
        }
    }

    #[rstest]
    #[case::empty_i8(r#"["i8", ""]"#, JoltVector {
        inner_type: JoltVectorType::Int8,
        data: vec![],
    })]
    #[case::empty_i16(r#"["i16", ""]"#, JoltVector {
        inner_type: JoltVectorType::Int16,
        data: vec![],
    })]
    #[case::empty_i32(r#"["i32", ""]"#, JoltVector {
        inner_type: JoltVectorType::Int32,
        data: vec![],
    })]
    #[case::empty_i64(r#"["i64", ""]"#, JoltVector {
        inner_type: JoltVectorType::Int64,
        data: vec![],
    })]
    #[case::empty_f32(r#"["f32", ""]"#, JoltVector {
        inner_type: JoltVectorType::Float32,
        data: vec![],
    })]
    #[case::empty_f64(r#"["f64", ""]"#, JoltVector {
        inner_type: JoltVectorType::Float64,
        data: vec![],
    })]
    #[case::data_i8(r#"["i8", "0F"]"#, JoltVector {
        inner_type: JoltVectorType::Int8,
        data: vec![0x0F],
    })]
    #[case::data_i16(r#"["i16", "0F 1F"]"#, JoltVector {
        inner_type: JoltVectorType::Int16,
        data: vec![0x0F, 0x1F],
    })]
    #[case::data_i32(r#"["i32", "0F 1F 2F 3F"]"#, JoltVector {
        inner_type: JoltVectorType::Int32,
        data: vec![0x0F, 0x1F, 0x2F, 0x3F],
    })]
    #[case::data_i64(r#"["i64", "0F 1F 2F 3F 4F 5F 6F 7F"]"#, JoltVector {
        inner_type: JoltVectorType::Int64,
        data: vec![0x0F, 0x1F, 0x2F, 0x3F, 0x4F, 0x5F, 0x6F, 0x7F],
    })]
    #[case::data_f32(r#"["f32", "0F 1F 2F 3F"]"#, JoltVector {
        inner_type: JoltVectorType::Float32,
        data: vec![0x0F, 0x1F, 0x2F, 0x3F],
    })]
    #[case::data_f64(r#"["f64", "0F 1F 2F 3F 4F 5F 6F 7F"]"#, JoltVector {
        inner_type: JoltVectorType::Float64,
        data: vec![0x0F, 0x1F, 0x2F, 0x3F, 0x4F, 0x5F, 0x6F, 0x7F],
    })]
    fn test_jolt_vector_parse(#[case] input: &str, #[case] expected: JoltVector) {
        let json = serde_json::from_str(input).unwrap();
        let jolt_version = JoltVersion::V3;
        let config = actor_config();

        let result = JoltVector::parse(json, jolt_version, &config).unwrap();

        assert_eq!(result.inner_type, expected.inner_type);
        assert_eq!(result.data, expected.data);
    }

    #[rstest]
    #[case::wrong_type(r#"1"#)]
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

    #[rstest]
    #[case::v1(JoltVersion::V1)]
    #[case::v2(JoltVersion::V2)]
    fn test_jolt_vector_parse_invalid_jolt_version(#[case] jolt_version: JoltVersion) {
        let json = serde_json::from_str(r#"["i8", ""]"#).unwrap();
        let config = actor_config();

        JoltVector::parse(json, jolt_version, &config).unwrap_err();
    }

    #[rstest]
    #[case::empty_i8(
        JoltVector {
            inner_type: JoltVectorType::Int8,
            data: vec![],
        },
        r#"{"V": ["i8", ""]}"#,
    )]
    #[case::empty_i16(
        JoltVector {
            inner_type: JoltVectorType::Int16,
            data: vec![],
        },
        r#"{"V": ["i16", ""]}"#,
    )]
    #[case::empty_i32(
        JoltVector {
            inner_type: JoltVectorType::Int32,
            data: vec![],
        },
        r#"{"V": ["i32", ""]}"#,
    )]
    #[case::empty_i64(
        JoltVector {
            inner_type: JoltVectorType::Int64,
            data: vec![],
        },
        r#"{"V": ["i64", ""]}"#,
    )]
    #[case::empty_f32(
        JoltVector {
            inner_type: JoltVectorType::Float32,
            data: vec![],
        },
        r#"{"V": ["f32", ""]}"#,
    )]
    #[case::empty_f64(
        JoltVector {
            inner_type: JoltVectorType::Float64,
            data: vec![],
        },
        r#"{"V": ["f64", ""]}"#,
    )]
    #[case::data_i8(
        JoltVector {
            inner_type: JoltVectorType::Int8,
            data: vec![0x0F],
        },
        r#"{"V": ["i8", "0F"]}"#,
    )]
    #[case::data_i16(
        JoltVector {
            inner_type: JoltVectorType::Int16,
            data: vec![0x0F, 0x1F],
        },
        r#"{"V": ["i16", "0F 1F"]}"#,
    )]
    #[case::data_i32(
        JoltVector {
            inner_type: JoltVectorType::Int32,
            data: vec![0x0F, 0x1F, 0x2F, 0x3F],
        },
        r#"{"V": ["i32", "0F 1F 2F 3F"]}"#,
    )]
    #[case::data_i64(
        JoltVector {
            inner_type: JoltVectorType::Int64,
            data: vec![0x0F, 0x1F, 0x2F, 0x3F, 0x4F, 0x5F, 0x6F, 0x7F],
        },
        r#"{"V": ["i64", "0F 1F 2F 3F 4F 5F 6F 7F"]}"#,
    )]
    #[case::data_f32(
        JoltVector {
            inner_type: JoltVectorType::Float32,
            data: vec![0x0F, 0x1F, 0x2F, 0x3F],
        },
        r#"{"V": ["f32", "0F 1F 2F 3F"]}"#,
    )]
    #[case::data_f64(
        JoltVector {
            inner_type: JoltVectorType::Float64,
            data: vec![0x0F, 0x1F, 0x2F, 0x3F, 0x4F, 0x5F, 0x6F, 0x7F],
        },
        r#"{"V": ["f64", "0F 1F 2F 3F 4F 5F 6F 7F"]}"#,
    )]
    fn test_jolt_fmt(#[case] jolt_vector: JoltVector, #[case] expected: &str) {
        let jolt_version = JoltVersion::V3;
        let bolt_vector = BoltVector(jolt_vector);
        let formatted = bolt_vector.jolt_fmt(jolt_version).to_string();
        assert_eq!(formatted, expected);
    }
}
