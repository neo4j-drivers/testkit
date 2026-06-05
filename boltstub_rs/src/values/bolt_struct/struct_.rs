use std::borrow::Cow;
use std::cell::LazyCell;
use std::fmt::{Debug, Display, Formatter};

use regex::Regex;
use serde::ser::SerializeSeq;
use serde::Serialize;
use serde_json::Value as JsonValue;

use super::_parsing::next_json_field;
use crate::bolt_version::JoltVersion;
use crate::jolt::JoltSigil;
use crate::parse_error::ParseError;
use crate::parser::ActorConfig;
use crate::values::bolt_struct::_parsing::iter_json_fields;
use crate::values::jolt_ser::{JoltSer, JoltSigilMapSer};
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

const SIGIL: &str = JoltSigil::Struct.str();

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct JoltStructData<'a> {
    pub(crate) tag: u8,
    pub(crate) fields: Cow<'a, [PackStreamValue]>,
}

impl JoltStructData<'static> {
    pub(crate) fn parse(
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<Self, ParseError> {
        let JsonValue::Array(fields) = v else {
            return Err(ParseError::new(format!(
                "Expected array after sigil \"{SIGIL}\", but found {v:?}"
            )));
        };
        let fields_count = fields.len().checked_sub(1).ok_or_else(|| {
            ParseError::new(format!(
                "Expected at least a tag value after sigil \"{SIGIL}\", but found {}",
                fields.len()
            ))
        })?;
        if fields_count > 15 {
            return Err(ParseError::new(format!(
                "Too many fields for a Struct after sigil \"{SIGIL}\", \
                {jolt_version} only supports up to 15 fields, found {fields_count}"
            )));
        }
        let (i, mut json_fields) = iter_json_fields(fields);

        let (mut i, tag) = next_json_field::<String>(&mut json_fields, "tag", i, SIGIL, config)?;
        let tag = Self::parse_tag(&tag)?;
        let mut fields = Vec::with_capacity(fields_count);
        for _ in 0..fields_count {
            let (new_i, field) = next_json_field(&mut json_fields, "field", i, SIGIL, config)?;
            i = new_i;
            fields.push(field);
        }
        let fields = Cow::Owned(fields);
        Ok(Self { tag, fields })
    }

    fn parse_tag(tag: &str) -> Result<u8, ParseError> {
        thread_local! {
            static TAG_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(r"^(?:0x)?([0-9a-fA-F]{2})$").unwrap()
            });
        }
        let captures = TAG_RE.with(|re| re.captures(tag)).ok_or_else(|| {
            ParseError::new(format!(
                "Unable to parse Struct tag {tag:?} after sigil \"{SIGIL}\", \
                expected single-byte hex string (e.g., \"0xAF\", \"7b\")",
            ))
        })?;
        let tag = captures
            .get(1)
            .expect("regex enforces existence of hex tag")
            .as_str();
        Ok(u8::from_str_radix(tag, 16).expect("regex enforces valid u8 hex tag"))
    }
}

impl JoltStructData<'_> {
    pub(super) fn jolt_serialize<S>(
        &self,
        serializer: S,
        jolt_version_data: JoltVersion,
        jolt_version_ctx: JoltVersion,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        fn tag_fmt(tag: u8) -> impl serde::Serialize {
            struct TagFmt(u8);

            impl Display for TagFmt {
                fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{:#04X}", self.0)
                }
            }

            impl serde::Serialize for TagFmt {
                fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
                where
                    S: serde::Serializer,
                {
                    serializer.collect_str(self)
                }
            }

            TagFmt(tag)
        }

        struct DataSer<'a> {
            data: &'a JoltStructData<'a>,
            jolt_version_ctx: JoltVersion,
        }

        impl serde::Serialize for DataSer<'_> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let size = self.data.fields.len() + 1;
                let mut seq = serializer.serialize_seq(Some(size))?;
                seq.serialize_element(&tag_fmt(self.data.tag))?;
                for field in self.data.fields.iter() {
                    seq.serialize_element(&JoltSer::new(field, self.jolt_version_ctx))?;
                }
                seq.end()
            }
        }

        let body = DataSer {
            data: self,
            jolt_version_ctx,
        };
        let map = JoltSigilMapSer::new(SIGIL, jolt_version_data, jolt_version_ctx, &body);
        map.serialize(serializer)
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct JoltStruct<'a> {
    pub(crate) data: JoltStructData<'a>,
    pub(super) jolt_version: JoltVersion,
}

impl JoltStruct<'static> {
    pub(crate) fn parse(
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<Self, ParseError> {
        JoltStructData::parse(v, jolt_version, config).map(|data| Self { data, jolt_version })
    }
}

impl<'a> JoltStruct<'a> {
    pub(crate) fn from_struct(s: &'a PackStreamStruct, jolt_version: JoltVersion) -> Self {
        let tag = s.tag;
        let fields = Cow::Borrowed(&*s.fields);

        let data = JoltStructData { tag, fields };
        JoltStruct { data, jolt_version }
    }

    pub(crate) fn jolt_serialize<S>(
        &self,
        serializer: S,
        jolt_version: JoltVersion,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.data
            .jolt_serialize(serializer, self.jolt_version, jolt_version)
    }

    pub(crate) fn into_struct(self) -> PackStreamStruct {
        PackStreamStruct {
            tag: self.data.tag,
            fields: self.data.fields.into_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;

    use crate::bolt_version::BoltCapabilities;
    use crate::ext::serde_json::support_pub::JoltSerializer;
    use crate::values::tests::{all_jolt_versions, jolt_serializer};

    use rstest::rstest;
    use rstest_reuse::apply;

    use super::*;

    fn make_actor_config(jolt_version: JoltVersion) -> ActorConfig {
        let bolt_version = jolt_version.min_bolt_version();
        let bolt_version_raw = (bolt_version.major(), bolt_version.major());
        ActorConfig {
            bolt_version,
            bolt_version_raw,
            bolt_capabilities: BoltCapabilities::default(),
            handshake_manifest_version: None,
            handshake: None,
            handshake_response: None,
            handshake_delay: None,
            allow_restart: false,
            allow_concurrent: false,
            auto_responses: IndexMap::default(),
            py_lines: vec![],
        }
    }

    #[rstest]
    #[case(r#"["00"]"#, JoltStructData { tag: 0x00, fields: (&[]).into() })]
    #[case(r#"["FF"]"#, JoltStructData { tag: 0xFF, fields: (&[]).into() })]
    #[case(r#"["0xFF"]"#, JoltStructData { tag: 0xFF, fields: (&[]).into() })]
    #[case(r#"["ff"]"#, JoltStructData { tag: 0xFF, fields: (&[]).into() })]
    #[case(r#"["0xff"]"#, JoltStructData { tag: 0xFF, fields: (&[]).into() })]
    #[case(
        r#"["0xff", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]"#,
        JoltStructData{
            tag: 0xFF,
            fields: (1..=15).map(Into::into).collect::<Vec<_>>().into()
        },
    )]
    #[case(
        r#"["0xff", {"U": "hi"}]"#,
        JoltStructData{
            tag: 0xFF,
            fields: vec!["hi".into()].into()
        },
    )]
    fn test_jolt_struct_parse(
        #[case] input: &str,
        #[case] expected: JoltStructData<'_>,
        #[values(JoltVersion::V1, JoltVersion::V2, JoltVersion::V3, JoltVersion::V4)]
        jolt_version: JoltVersion,
    ) {
        let json = serde_json::from_str(input).unwrap();
        let config = make_actor_config(jolt_version);

        let result = JoltStruct::parse(json, jolt_version, &config).unwrap();

        let expected = JoltStruct {
            data: expected,
            jolt_version,
        };
        assert_eq!(result, expected);
    }

    #[apply(all_jolt_versions)]
    #[case::wrong_type("{}")]
    #[case::array_too_short("[]")]
    #[case::array_too_long(r#"["00", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]"#)]
    #[case::wrong_tag_type("[1]")]
    #[case::invalid_tag_format(r#"["\\x00"]"#)]
    #[case::invalid_tag_format(r#"["255"]"#)]
    #[case::invalid_tag_format(r#"["0xFFFF"]"#)]
    fn test_jolt_struct_parse_invalid(#[case] input: &str, jolt_version: JoltVersion) {
        let json = serde_json::from_str(input).unwrap();
        let config = make_actor_config(jolt_version);

        JoltStruct::parse(json, jolt_version, &config).unwrap_err();
    }

    #[apply(all_jolt_versions)]
    #[case::empty(
        JoltStruct {
            data: JoltStructData {
                tag: 0x00,
                fields: Cow::Borrowed(&[]),
            },
            jolt_version: JoltVersion::V3,
        },
        r#"{"STRUCT": ["0x00"]}"#,
        r#"{"STRUCTv3": ["0x00"]}"#,
    )]
    #[case::some_data(
        JoltStruct {
            data: JoltStructData {
                tag: 0xFF,
                fields: Cow::Owned(vec![r#"hi\"\"#.into(), 1.into()]),
            },
            jolt_version: JoltVersion::V1,
        },
        r#"{"STRUCT": ["0xFF", "hi\\\"\\", 1]}"#,
        r#"{"STRUCTv1": ["0xFF", "hi\\\"\\", 1]}"#,
    )]
    fn test_jolt_fmt(
        #[case] jolt_struct: JoltStruct,
        #[case] expected: &str,
        #[case] expected_versioned: &str,
        jolt_version: JoltVersion,
        mut jolt_serializer: JoltSerializer<Vec<u8>>,
    ) {
        jolt_struct
            .jolt_serialize(jolt_serializer.ser(), jolt_version)
            .expect("Failed to serialize Jolt");
        let formatted = jolt_serializer.into_string();

        if jolt_struct.jolt_version == jolt_version {
            assert_eq!(formatted, expected);
        } else {
            assert_eq!(formatted, expected_versioned);
        }
    }

    #[rstest]
    #[case(
        JoltStructData { tag: 0, fields: Cow::Borrowed(&[]) },
        PackStreamStruct { tag: 0, fields: vec![] },
    )]
    #[case(
        JoltStructData { tag: 0xFF, fields: Cow::Owned(vec![]) },
        PackStreamStruct { tag: 0xFF, fields: vec![] },
    )]
    #[case(
        JoltStructData { tag: 0xFF, fields: Cow::Owned(vec![1.into(), "".into()]) },
        PackStreamStruct { tag: 0xFF, fields: vec![1.into(), "".into()] },
    )]
    fn test_to_struct(
        #[case] data: JoltStructData<'_>,
        #[case] expected: PackStreamStruct,
        #[values(JoltVersion::V1, JoltVersion::V2, JoltVersion::V3, JoltVersion::V4)]
        jolt_version: JoltVersion,
    ) {
        let jolt_struct = JoltStruct { data, jolt_version };

        let struct_ = jolt_struct.into_struct();

        assert_eq!(struct_, expected);
    }

    #[rstest]
    #[case(
        JoltStructData { tag: 0, fields: Cow::Borrowed(&[]) },
        PackStreamStruct { tag: 0, fields: vec![] },
    )]
    #[case(
        JoltStructData { tag: 0xFF, fields: Cow::Owned(vec![]) },
        PackStreamStruct { tag: 0xFF, fields: vec![] },
    )]
    #[case(
        JoltStructData { tag: 0xFF, fields: Cow::Owned(vec![1.into(), "".into()]) },
        PackStreamStruct { tag: 0xFF, fields: vec![1.into(), "".into()] },
    )]
    fn test_from_struct(
        #[case] expected: JoltStructData<'_>,
        #[case] struct_: PackStreamStruct,
        #[values(JoltVersion::V1, JoltVersion::V2, JoltVersion::V3, JoltVersion::V4)]
        jolt_version: JoltVersion,
    ) {
        let jolt_struct = JoltStruct::from_struct(&struct_, jolt_version);

        let expected = JoltStruct {
            data: expected,
            jolt_version,
        };
        assert_eq!(jolt_struct, expected);
    }
}
