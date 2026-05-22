use std::borrow::Cow;
use std::fmt::{Debug, Display, Formatter};
use std::mem;

use indexmap::IndexMap;
use serde_json::Value as JsonValue;

use super::_parsing::{
    check_last_json_field, check_last_pack_stream_field, iter_json_fields, next_json_field,
    next_pack_stream_field,
};
use crate::bolt_version::JoltVersion;
use crate::jolt::JoltSigil;
use crate::parse_error::ParseError;
use crate::parser::ActorConfig;
use crate::values::bolt_struct::_common::element_id::ElementIdExt;
use crate::values::bolt_struct::_common::fmt_jolt_sigil_and_map;
use crate::values::bolt_struct::{TAG_RELATIONSHIP, TAG_UNBOUND_RELATIONSHIP};
use crate::values::pack_stream_value::{write_joined_entries, PackStreamStruct, PackStreamValue};

const SIGIL_F: &str = JoltSigil::RelationshipForward.str();
const SIGIL_B: &str = JoltSigil::RelationshipBackward.str();
const SIGIL: &str = constcat::concat!(SIGIL_F, r#""/""#, SIGIL_B);

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct JoltRelationshipData<'a> {
    pub(crate) id: i64,
    pub(crate) start_node_id: i64,
    pub(crate) rel_type: Cow<'a, str>,
    pub(crate) end_node_id: i64,
    pub(crate) properties: Cow<'a, IndexMap<String, PackStreamValue>>,
    pub(crate) element_id_ext: ElementIdExt<JoltRelationshipElementIdExt<'a>>,
}

#[allow(clippy::struct_field_names, reason = "for better readability")]
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct JoltRelationshipElementIdExt<'a> {
    pub(crate) element_id: Cow<'a, str>,
    pub(crate) start_node_element_id: Cow<'a, str>,
    pub(crate) end_node_element_id: Cow<'a, str>,
}

impl JoltRelationshipData<'static> {
    pub(super) fn parse(
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<Self, ParseError> {
        let JsonValue::Array(fields) = v else {
            return Err(ParseError::new(format!(
                "Expected array after sigil \"{SIGIL}\", \
                but found {v:?}"
            )));
        };
        let (i, mut fields) = iter_json_fields(fields);

        let (i, id) = next_json_field(&mut fields, "id", i, SIGIL, config)?;
        let (i, start_node_id) = next_json_field(&mut fields, "start node id", i, SIGIL, config)?;
        let (i, rel_type) = next_json_field(&mut fields, "relationship type", i, SIGIL, config)?;
        let (i, end_node_id) = next_json_field(&mut fields, "end node id", i, SIGIL, config)?;
        let (i, properties) = next_json_field(&mut fields, "properties", i, SIGIL, config)?;
        let (i, element_id_ext) = {
            let mut i = i;
            let element_id_ext = ElementIdExt::new_lazy(jolt_version, || {
                let (new_i, element_id) =
                    next_json_field(&mut fields, "element id", i, SIGIL, config)?;
                i = new_i;
                let (new_i, start_node_element_id) =
                    next_json_field(&mut fields, "start node element id", i, SIGIL, config)?;
                i = new_i;
                let (new_i, end_node_element_id) =
                    next_json_field(&mut fields, "end node element id", i, SIGIL, config)?;
                i = new_i;
                Ok::<_, ParseError>(JoltRelationshipElementIdExt {
                    element_id: Cow::Owned(element_id),
                    start_node_element_id: Cow::Owned(start_node_element_id),
                    end_node_element_id: Cow::Owned(end_node_element_id),
                })
            })
            .transpose()?;
            (i, element_id_ext)
        };
        check_last_json_field(&mut fields, i, "{}")?;

        Ok(JoltRelationshipData {
            id,
            start_node_id,
            rel_type: Cow::Owned(rel_type),
            end_node_id,
            properties: Cow::Owned(properties),
            element_id_ext,
        })
    }
}

impl JoltRelationshipData<'_> {
    pub(crate) fn flip_direction(&mut self) {
        mem::swap(&mut self.start_node_id, &mut self.end_node_id);
        self.element_id_ext.apply(|ext| {
            mem::swap(&mut ext.start_node_element_id, &mut ext.end_node_element_id);
        });
    }

    pub(super) fn jolt_fmt(
        &self,
        jolt_version_data: JoltVersion,
        jolt_version_ctx: JoltVersion,
    ) -> impl Display + '_ {
        struct JoltFormatter<'a> {
            data: &'a JoltRelationshipData<'a>,
            jolt_version_data: JoltVersion,
            jolt_version_ctx: JoltVersion,
        }

        impl Display for JoltFormatter<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                fmt_jolt_sigil_and_map(
                    f,
                    SIGIL_F,
                    self.jolt_version_data,
                    self.jolt_version_ctx,
                    Some(("[", "]")),
                    |f| {
                        Display::fmt(&self.data.id, f)?;
                        f.write_str(", ")?;
                        Display::fmt(&self.data.start_node_id, f)?;
                        f.write_str(", \"")?;
                        Display::fmt(&self.data.rel_type, f)?;
                        f.write_str("\", ")?;
                        Display::fmt(&self.data.end_node_id, f)?;
                        f.write_str(", {")?;
                        let properties = self.data.properties.iter().map(|(k, v)| (k.as_str(), v));
                        write_joined_entries(f, properties, self.jolt_version_ctx)?;
                        match self.data.element_id_ext.inner(self.jolt_version_data) {
                            None => f.write_str("}"),
                            Some(ext) => {
                                f.write_str("}, \"")?;
                                Display::fmt(&ext.element_id, f)?;
                                f.write_str("\", \"")?;
                                Display::fmt(&ext.start_node_element_id, f)?;
                                f.write_str("\", \"")?;
                                Display::fmt(&ext.end_node_element_id, f)?;
                                f.write_str("\"")
                            }
                        }
                    },
                )
            }
        }

        JoltFormatter {
            data: self,
            jolt_version_data,
            jolt_version_ctx,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct JoltRelationship<'a> {
    pub(crate) data: JoltRelationshipData<'a>,
    pub(crate) jolt_version: JoltVersion,
}

impl JoltRelationship<'static> {
    pub(crate) fn parse(
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<Self, ParseError> {
        JoltRelationshipData::parse(v, jolt_version, config).map(|data| Self { data, jolt_version })
    }
}

impl<'a> JoltRelationship<'a> {
    pub(crate) fn flip_direction(&mut self) {
        self.data.flip_direction();
    }

    pub(crate) fn into_struct(self) -> PackStreamStruct {
        let mut fields = Vec::with_capacity(8);
        fields.extend([
            PackStreamValue::Integer(self.data.id),
            PackStreamValue::Integer(self.data.start_node_id),
            PackStreamValue::Integer(self.data.end_node_id),
            PackStreamValue::String(self.data.rel_type.into_owned()),
            PackStreamValue::Dict(self.data.properties.into_owned()),
        ]);
        if let Some(element_id_ext) = self.data.element_id_ext.into_inner(self.jolt_version) {
            fields.push(PackStreamValue::String(
                element_id_ext.element_id.into_owned(),
            ));
            fields.push(PackStreamValue::String(
                element_id_ext.start_node_element_id.into_owned(),
            ));
            fields.push(PackStreamValue::String(
                element_id_ext.end_node_element_id.into_owned(),
            ));
        }
        PackStreamStruct {
            tag: TAG_RELATIONSHIP,
            fields,
        }
    }

    pub(super) fn from_struct(s: &'a PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        let PackStreamStruct { tag, fields } = s;
        if *tag != TAG_RELATIONSHIP {
            return None;
        }
        let mut fields = fields.iter();
        let id = next_pack_stream_field(&mut fields)?;
        let start_node_id = next_pack_stream_field(&mut fields)?;
        let end_node_id = next_pack_stream_field(&mut fields)?;
        let rel_type = Cow::Borrowed(next_pack_stream_field(&mut fields)?);
        let properties = Cow::Borrowed(next_pack_stream_field(&mut fields)?);
        let element_id_ext = ElementIdExt::new_lazy(jolt_version, || {
            let element_id = Cow::Borrowed(next_pack_stream_field(&mut fields)?);
            let start_node_element_id = Cow::Borrowed(next_pack_stream_field(&mut fields)?);
            let end_node_element_id = Cow::Borrowed(next_pack_stream_field(&mut fields)?);
            Some(JoltRelationshipElementIdExt {
                element_id,
                start_node_element_id,
                end_node_element_id,
            })
        })
        .transpose()?;
        if !check_last_pack_stream_field(&mut fields) {
            return None;
        }
        Some(Self {
            data: JoltRelationshipData {
                id,
                start_node_id,
                rel_type,
                end_node_id,
                properties,
                element_id_ext,
            },
            jolt_version,
        })
    }

    pub(super) fn jolt_fmt(&self, jolt_version: JoltVersion) -> impl Display + '_ {
        self.data.jolt_fmt(self.jolt_version, jolt_version)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct JoltUnboundRelationshipData<'a> {
    pub(crate) id: i64,
    pub(crate) rel_type: Cow<'a, str>,
    pub(crate) properties: Cow<'a, IndexMap<String, PackStreamValue>>,
    pub(crate) element_id: ElementIdExt<Cow<'a, str>>,
}

impl<'a> From<JoltRelationshipData<'a>> for JoltUnboundRelationshipData<'a> {
    fn from(value: JoltRelationshipData<'a>) -> Self {
        Self {
            id: value.id,
            rel_type: value.rel_type,
            properties: value.properties,
            element_id: value.element_id_ext.map(|ext| ext.element_id),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct JoltUnboundRelationship<'a> {
    pub(crate) data: JoltUnboundRelationshipData<'a>,
    pub(crate) jolt_version: JoltVersion,
}

impl<'a> From<JoltRelationship<'a>> for JoltUnboundRelationship<'a> {
    fn from(value: JoltRelationship<'a>) -> Self {
        Self {
            data: value.data.into(),
            jolt_version: value.jolt_version,
        }
    }
}

impl<'a> JoltUnboundRelationship<'a> {
    pub(super) fn new(data: JoltUnboundRelationshipData<'a>, jolt_version: JoltVersion) -> Self {
        data.element_id.assert_jolt_version(jolt_version);
        Self { data, jolt_version }
    }

    pub(crate) fn into_struct(self) -> PackStreamStruct {
        let mut fields = Vec::with_capacity(4);
        fields.extend([
            PackStreamValue::Integer(self.data.id),
            PackStreamValue::String(self.data.rel_type.into_owned()),
            PackStreamValue::Dict(self.data.properties.into_owned()),
        ]);
        if let Some(element_id) = self.data.element_id.into_inner(self.jolt_version) {
            fields.push(PackStreamValue::String(element_id.into_owned()));
        }
        PackStreamStruct {
            tag: TAG_UNBOUND_RELATIONSHIP,
            fields,
        }
    }

    pub(super) fn from_struct(s: &'a PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        if s.tag != TAG_UNBOUND_RELATIONSHIP {
            return None;
        }
        let mut fields = s.fields.iter();
        let id = next_pack_stream_field(&mut fields)?;
        let rel_type = Cow::Borrowed(next_pack_stream_field(&mut fields)?);
        let properties = Cow::Borrowed(next_pack_stream_field(&mut fields)?);
        let element_id = ElementIdExt::new_lazy(jolt_version, || {
            Some(Cow::Borrowed(next_pack_stream_field(&mut fields)?))
        })
        .transpose()?;
        if !check_last_pack_stream_field(&mut fields) {
            return None;
        }
        let data = JoltUnboundRelationshipData {
            id,
            rel_type,
            properties,
            element_id,
        };
        Some(Self { data, jolt_version })
    }
}

#[cfg(test)]
mod tests {
    use crate::bolt_version::BoltCapabilities;

    use super::*;

    use rstest::rstest;

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

    fn make_element_ids<'a>(
        jolt_version: JoltVersion,
        ids: (&'a str, &'a str, &'a str),
    ) -> ElementIdExt<JoltRelationshipElementIdExt<'a>> {
        let ext = JoltRelationshipElementIdExt {
            element_id: Cow::Borrowed(ids.0),
            start_node_element_id: Cow::Borrowed(ids.1),
            end_node_element_id: Cow::Borrowed(ids.2),
        };
        ElementIdExt::new(jolt_version, ext)
    }

    #[rstest]
    #[case(
        r#"[123, 456, "FOO", 786, {"a": 1, "b": null}]"#,
        (123, 456, 786),
        "FOO",
        &[("a", 1.into()), ("b", None::<i64>.into())],
    )]
    #[case(
        r#"[-1, 9223372036854775807, "", -9223372036854775808, {}]"#,
        (-1, i64::MAX, i64::MIN),
        "",
        &[],
    )]
    #[case(
        r#"[0, 0, "🍨", 0, {}]"#,
        (0, 0, 0),
        "🍨",
        &[],
    )]
    fn test_parse_v1(
        #[case] input: &str,
        #[case] ids: (i64, i64, i64),
        #[case] rel_type: &str,
        #[case] properties: &[(&str, PackStreamValue)],
    ) {
        let jolt_version = JoltVersion::V1;
        let config = make_actor_config(jolt_version);
        let input = serde_json::from_str(input).unwrap();
        let properties = properties
            .iter()
            .map(|(k, v)| ((*k).into(), v.clone()))
            .collect();

        let parsed =
            JoltRelationship::parse(input, jolt_version, &config).expect("case input failed");

        let data = JoltRelationshipData {
            id: ids.0,
            start_node_id: ids.1,
            rel_type: rel_type.into(),
            end_node_id: ids.2,
            properties: Cow::Owned(properties),
            element_id_ext: make_element_ids(jolt_version, ("", "", "")),
        };
        let expected = JoltRelationship { data, jolt_version };
        assert_eq!(parsed, expected);
    }

    #[rstest]
    #[case(
        r#"[123, 456, "FOO", 786, {"a": 1, "b": null}, "", "", ""]"#,
        (123, 456, 786),
        "FOO",
        &[("a", 1.into()), ("b", None::<i64>.into())],
        ("", "", ""),
    )]
    #[case(
        r#"[-1, 9223372036854775807, "", -9223372036854775808, {}, "a", "b", "c"]"#,
        (-1, i64::MAX, i64::MIN),
        "",
        &[],
        ("a", "b", "c"),
    )]
    #[case(
        r#"[0, 0, "🍨", 0, {"🚑": "🏥"}, "🍪", "🍩", "⚖️"]"#,
        (0, 0, 0),
        "🍨",
        &[("🚑", "🏥".into())],
        ("🍪", "🍩", "⚖️")
    )]
    fn test_parse_v2_plus(
        #[case] input: &str,
        #[case] ids: (i64, i64, i64),
        #[case] rel_type: &str,
        #[case] properties: &[(&str, PackStreamValue)],
        #[case] element_ids: (&str, &str, &str),
        #[values(JoltVersion::V2, JoltVersion::V3, JoltVersion::V4)] jolt_version: JoltVersion,
    ) {
        let config = make_actor_config(jolt_version);
        let input = serde_json::from_str(input).unwrap();
        let properties = properties
            .iter()
            .map(|(k, v)| ((*k).into(), v.clone()))
            .collect();
        let element_id_ext = make_element_ids(jolt_version, element_ids);

        let parsed =
            JoltRelationship::parse(input, jolt_version, &config).expect("case input failed");

        let data = JoltRelationshipData {
            id: ids.0,
            start_node_id: ids.1,
            rel_type: rel_type.into(),
            end_node_id: ids.2,
            properties: Cow::Owned(properties),
            element_id_ext,
        };
        let expected = JoltRelationship { data, jolt_version };
        assert_eq!(parsed, expected);
    }

    #[rstest]
    #[case(
        r#"["1", 2, "FOO", 3, {"a": 1, "b": null}]"#,
        &["id (field 1)", "to be an integer", "found string"],
    )]
    #[case(
        r#"[1, "2", "FOO", 3, {"a": 1, "b": null}]"#,
        &["start node id (field 2)", "to be an integer", "found string"],
    )]
    #[case(
        r#"[1, 2, 0, 3, {"a": 1, "b": null}]"#,
        &["relationship type (field 3)", "to be a string", "found number"],
    )]
    #[case(
        r#"[1, 2, {"0": "FOO"}, 3, {"a": 1, "b": null}]"#,
        &["relationship type (field 3)", "to be a string", "found object"],
    )]
    #[case(
        r#"[1, 2, "FOO", "3", {"a": 1, "b": null}]"#,
        &["end node id (field 4)", "to be an integer", "found string"],
    )]
    #[case(
        r#"[1, 2, "", 3, null]"#,
        &["properties (field 5)", "to be a map", "found null"],
    )]
    #[case(
        "[]",
        &["missing", "id (field 1)"],
    )]
    #[case(
        "[1]",
        &["missing", "start node id (field 2)"],
    )]
    #[case(
        "[1, 2]",
        &["missing", "relationship type (field 3)"],
    )]
    #[case(
        r#"[1, 2, ""]"#,
        &["missing", "end node id (field 4)"],
    )]
    #[case(
        r#"[1, 2, "", 3]"#,
        &["missing", "properties (field 5)"],
    )]
    #[case(
        r#"[1, 2, "", 3, {}, null]"#,
        &["too many fields"],
    )]
    fn test_failing_parse_v1(#[case] input: &str, #[case] err_matchers: &[&str]) {
        let jolt_version = JoltVersion::V1;
        let config = make_actor_config(jolt_version);
        let input = serde_json::from_str(input).unwrap();

        let err = JoltRelationship::parse(input, jolt_version, &config)
            .expect_err("case input parsed successfully");
        for error_matcher in err_matchers {
            assert!(
                err.to_string().to_lowercase().contains(error_matcher),
                "Expected error message to contain {error_matcher:?}, but got {err:?}"
            );
        }
    }

    #[rstest]
    #[case(
        r#"["1", 2, "FOO", 3, {"a": 1, "b": null}, "e1", "e2", "e3"]"#,
        &["id (field 1)", "to be an integer", "found string"],
    )]
    #[case(
        r#"[1, "2", "FOO", 3, {"a": 1, "b": null}, "e1", "e2", "e3"]"#,
        &["start node id (field 2)", "to be an integer", "found string"],
    )]
    #[case(
        r#"[1, 2, 0, 3, {"a": 1, "b": null}, "e1", "e2", "e3"]"#,
        &["relationship type (field 3)", "to be a string", "found number"],
    )]
    #[case(
        r#"[1, 2, {"0": "FOO"}, 3, {"a": 1, "b": null}, "e1", "e2", "e3"]"#,
        &["relationship type (field 3)", "to be a string", "found object"],
    )]
    #[case(
        r#"[1, 2, "FOO", "3", {"a": 1, "b": null}, "e1", "e2", "e3"]"#,
        &["end node id (field 4)", "to be an integer", "found string"],
    )]
    #[case(
        r#"[1, 2, "", 3, null, "e1", "e2", "e3"]"#,
        &["properties (field 5)", "to be a map", "found null"],
    )]
    #[case(
        r#"[1, 2, "", 3, {}, 1, "e2", "e3"]"#,
        &["element id (field 6)", "to be a string", "found number"],
    )]
    #[case(
        r#"[1, 2, "", 3, {}, "e1", 2, "e3"]"#,
        &["start node element id (field 7)", "to be a string", "found number"],
    )]
    #[case(
        r#"[1, 2, "", 3, {}, "e1", "e2", 3]"#,
        &["end node element id (field 8)", "to be a string", "found number"],
    )]
    #[case(
        "[]",
        &["missing", "id (field 1)"],
    )]
    #[case(
        "[1]",
        &["missing", "start node id (field 2)"],
    )]
    #[case(
        "[1, 2]",
        &["missing", "relationship type (field 3)"],
    )]
    #[case(
        r#"[1, 2, ""]"#,
        &["missing", "end node id (field 4)"],
    )]
    #[case(
        r#"[1, 2, "", 3]"#,
        &["missing", "properties (field 5)"],
    )]
    #[case(
        r#"[1, 2, "", 3, {}]"#,
        &["missing", "element id (field 6)"],
    )]
    #[case(
        r#"[1, 2, "", 3, {}, "e1"]"#,
        &["missing", "start node element id (field 7)"],
    )]
    #[case(
        r#"[1, 2, "", 3, {}, "e1", "e2"]"#,
        &["missing", "end node element id (field 8)"],
    )]
    #[case(
        r#"[1, 2, "", 3, {}, "e1", "e2", "e3", null]"#,
        &["too many fields"],
    )]
    fn test_failing_parse_v2_plus(
        #[case] input: &str,
        #[case] err_matchers: &[&str],
        #[values(JoltVersion::V2, JoltVersion::V3, JoltVersion::V4)] jolt_version: JoltVersion,
    ) {
        let config = make_actor_config(jolt_version);
        let input = serde_json::from_str(input).unwrap();

        let err = JoltRelationship::parse(input, jolt_version, &config)
            .expect_err("case input parsed successfully");
        for error_matcher in err_matchers {
            assert!(
                err.to_string().to_lowercase().contains(error_matcher),
                "Expected error message to contain {error_matcher:?}, but got {err:?}"
            );
        }
    }

    #[rstest]
    #[case(
        r#"[0, 0, "", 0, {"uuid": {"UU": "12345678-1234-1234-1234-123456789012"}}]"#,
        JoltVersion::V1,
        false,
        false
    )]
    #[case(
        r#"[0, 0, "", 0, {"uuid": {"UU": "12345678-1234-1234-1234-123456789012"}}, "", "", ""]"#,
        JoltVersion::V4,
        false,
        true
    )]
    #[case(
        r#"[0, 0, "", 0, {"uuid": {"UUv4": "12345678-1234-1234-1234-123456789012"}}]"#,
        JoltVersion::V1,
        true,
        true
    )]
    #[case(
        r#"[0, 0, "", 0, {"uuid": {"UUv1": "12345678-1234-1234-1234-123456789012"}}, "", "", ""]"#,
        JoltVersion::V4,
        false,
        false
    )]
    fn test_parse_recursive_jolt_version(
        #[case] input: &str,
        #[case] jolt_version: JoltVersion,
        #[case] fails: bool,
        #[case] parses_uuid: bool,
    ) {
        let config = make_actor_config(jolt_version);
        let input = serde_json::from_str(input).unwrap();

        let res = JoltRelationship::parse(input, jolt_version, &config);
        if fails {
            res.expect_err("didn't fail parsing");
            return;
        }
        let JoltRelationship {
            data,
            jolt_version: parsed_jolt_version,
        } = res.expect("case input failed");

        assert_eq!(parsed_jolt_version, jolt_version);
        assert_eq!(data.properties.len(), 1);
        let uuid = data.properties.get("uuid").expect("missing uuid property");
        if parses_uuid {
            assert!(uuid.is_uuid());
        } else {
            let uuid_map = uuid.as_map().expect("not a map");
            assert_eq!(uuid_map.len(), 1);
            let key = uuid_map.keys().next().unwrap();
            assert!(key.starts_with("UU"));
            let uuid_str = uuid_map.get(key).expect("missing UU* key");
            assert!(uuid_str.is_string());
        }
    }

    #[rstest]
    #[case(
        r#"[123, 234, "FOO", 345, {"a": 1, "b": null}]"#,
        (123, 234, 345),
        "FOO",
        &[("a", 1.into()), ("b", None::<i64>.into())],
    )]
    #[case(
        r#"[0, 9223372036854775807, "", -9223372036854775808, {}]"#,
        (0, i64::MAX, i64::MIN),
        "",
        &[],
    )]
    fn test_jolt_fmt_v1(
        #[case] expected: &str,
        #[case] ids: (i64, i64, i64),
        #[case] rel_type: &str,
        #[case] properties: &[(&str, PackStreamValue)],
    ) {
        let jolt_version = JoltVersion::V1;
        let properties = properties
            .iter()
            .map(|(k, v)| ((*k).into(), v.clone()))
            .collect();
        let data = JoltRelationshipData {
            id: ids.0,
            start_node_id: ids.1,
            rel_type: rel_type.into(),
            end_node_id: ids.2,
            properties: Cow::Owned(properties),
            element_id_ext: make_element_ids(jolt_version, ("", "", "")),
        };
        let node = JoltRelationship { data, jolt_version };

        let expected = format!(r#"{{"->": {expected}}}"#);
        assert_eq!(node.jolt_fmt(jolt_version).to_string(), expected);
    }

    #[rstest]
    #[case(
        r#"[123, 234, "FOO", 345, {"a": 1, "b": null}, "", "", ""]"#,
        (123, 234, 345),
        "FOO",
        &[("a", 1.into()), ("b", None::<i64>.into())],
        ("", "", ""),
    )]
    #[case(
        r#"[0, 9223372036854775807, "", -9223372036854775808, {}, "🍪", "🍩", "⚖️"]"#,
        (0, i64::MAX, i64::MIN),
        "",
        &[],
        ("🍪", "🍩", "⚖️"),
    )]
    fn test_jolt_fmt_v2_plus(
        #[case] expected: &str,
        #[case] ids: (i64, i64, i64),
        #[case] rel_type: &str,
        #[case] properties: &[(&str, PackStreamValue)],
        #[case] element_ids: (&str, &str, &str),
        #[values(JoltVersion::V2, JoltVersion::V3, JoltVersion::V4)] jolt_version: JoltVersion,
    ) {
        let properties = properties
            .iter()
            .map(|(k, v)| ((*k).into(), v.clone()))
            .collect();
        let data = JoltRelationshipData {
            id: ids.0,
            start_node_id: ids.1,
            rel_type: rel_type.into(),
            end_node_id: ids.2,
            properties: Cow::Owned(properties),
            element_id_ext: make_element_ids(jolt_version, element_ids),
        };
        let node = JoltRelationship { data, jolt_version };

        let expected = format!(r#"{{"->": {expected}}}"#);
        assert_eq!(node.jolt_fmt(jolt_version).to_string(), expected);
    }

    #[rstest]
    #[case(
        (0, 0, 0),
        "",
        &[],
        ("", "", "")
    )]
    #[case(
        (1, 2, 3),
        "FOO",
        &[("a", None::<i64>.into()), ("c", 1.into()), ("b", vec![1, 2, 3].into())],
        ("abc", "def", "xyz")
    )]
    #[case(
        (i64::MIN, i64::MIN, i64::MIN),
        "🍨",
        &[("a", None::<i64>.into()), ("c", 1.into()), ("b", vec![1, 2, 3].into())],
        ("🍪", "🍩", "⚖️"),
    )]
    fn test_as_struct(
        #[case] ids: (i64, i64, i64),
        #[case] rel_type: &str,
        #[case] properties: &[(&str, PackStreamValue)],
        #[case] element_ids: (&str, &str, &str),
        #[values(JoltVersion::V1, JoltVersion::V2, JoltVersion::V3, JoltVersion::V4)]
        jolt_version: JoltVersion,
    ) {
        let properties: IndexMap<String, PackStreamValue> = properties
            .iter()
            .map(|(k, v)| ((*k).into(), v.clone()))
            .collect();
        let data = JoltRelationshipData {
            id: ids.0,
            start_node_id: ids.1,
            rel_type: rel_type.into(),
            end_node_id: ids.2,
            properties: Cow::Owned(properties.clone()),
            element_id_ext: make_element_ids(jolt_version, element_ids),
        };
        let node = JoltRelationship { data, jolt_version };

        let struct_ = node.into_struct();

        let mut fields = vec![
            ids.0.into(),
            ids.1.into(),
            ids.2.into(),
            rel_type.into(),
            properties.into(),
        ];
        if ElementIdExt::uses_element_id(jolt_version) {
            fields.push(element_ids.0.into());
            fields.push(element_ids.1.into());
            fields.push(element_ids.2.into());
        }
        let expected = PackStreamStruct {
            tag: TAG_RELATIONSHIP,
            fields,
        };
        assert_eq!(struct_, expected);
    }

    #[rstest]
    #[case(
        (0, 0, 0),
        "",
        &[],
        ("", "", "")
    )]
    #[case(
        (1, 2, 3),
        "FOO",
        &[("a", None::<i64>.into()), ("c", 1.into()), ("b", vec![1, 2, 3].into())],
        ("abc", "def", "xyz")
    )]
    #[case(
        (i64::MIN, i64::MIN, i64::MIN),
        "🍨",
        &[("a", None::<i64>.into()), ("c", 1.into()), ("b", vec![1, 2, 3].into())],
        ("🍪", "🍩", "⚖️"),
    )]
    fn test_from_struct(
        #[case] ids: (i64, i64, i64),
        #[case] rel_type: &str,
        #[case] properties: &[(&str, PackStreamValue)],
        #[case] element_ids: (&str, &str, &str),
        #[values(JoltVersion::V1, JoltVersion::V2, JoltVersion::V3, JoltVersion::V4)]
        jolt_version: JoltVersion,
    ) {
        let properties: IndexMap<String, PackStreamValue> = properties
            .iter()
            .map(|(k, v)| ((*k).into(), v.clone()))
            .collect();
        let mut fields = vec![
            ids.0.into(),
            ids.1.into(),
            ids.2.into(),
            rel_type.into(),
            properties.clone().into(),
        ];
        if ElementIdExt::uses_element_id(jolt_version) {
            fields.push(element_ids.0.into());
            fields.push(element_ids.1.into());
            fields.push(element_ids.2.into());
        }
        let struct_ = PackStreamStruct {
            tag: TAG_RELATIONSHIP,
            fields,
        };

        let struct_ =
            JoltRelationship::from_struct(&struct_, jolt_version).expect("failed to load");

        let data = JoltRelationshipData {
            id: ids.0,
            start_node_id: ids.1,
            rel_type: rel_type.into(),
            end_node_id: ids.2,
            properties: Cow::Owned(properties),
            element_id_ext: make_element_ids(jolt_version, element_ids),
        };
        let expected = JoltRelationship { data, jolt_version };

        assert_eq!(struct_, expected);
    }
}
