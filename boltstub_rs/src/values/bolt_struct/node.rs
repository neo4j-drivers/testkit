use std::borrow::Cow;
use std::fmt::{Debug, Display, Formatter};

use indexmap::IndexMap;
use itertools::Itertools;
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
use crate::values::bolt_struct::TAG_NODE;
use crate::values::pack_stream_value::{write_joined_entries, PackStreamStruct, PackStreamValue};

const SIGIL: &str = JoltSigil::Node.str();

#[derive(Debug, Clone)]
pub(crate) struct JoltNodeData<'a> {
    pub(crate) id: i64,
    pub(crate) labels: Vec<Cow<'a, str>>,
    pub(crate) properties: Cow<'a, IndexMap<String, PackStreamValue>>,
    pub(crate) element_id: ElementIdExt<Cow<'a, str>>,
}

impl PartialEq for JoltNodeData<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.properties == other.properties
            && self.element_id == other.element_id
            && self.labels.len() == other.labels.len()
            && self.labels.iter().sorted().eq(other.labels.iter().sorted())
    }
}

impl Eq for JoltNodeData<'_> {}

impl JoltNodeData<'static> {
    pub(super) fn parse(
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<Self, ParseError> {
        let JsonValue::Array(fields) = v else {
            return Err(ParseError::new(format!(
                "Expected array after sigil \"{SIGIL}\", but found {v:?}"
            )));
        };
        let (i, mut fields) = iter_json_fields(fields);

        let (i, id) = next_json_field(&mut fields, "node id", i, SIGIL, config)?;
        let (i, labels) = next_json_field::<Vec<String>>(&mut fields, "labels", i, SIGIL, config)?;
        let labels = labels.into_iter().map(Cow::Owned).collect();
        let (i, properties) = next_json_field(&mut fields, "properties", i, SIGIL, config)?;
        let (i, element_id) = {
            let mut i = i;
            let element_id = ElementIdExt::new_lazy(jolt_version, || {
                let (new_i, element_id) =
                    next_json_field(&mut fields, "element id", i, SIGIL, config)?;
                i = new_i;
                Ok::<_, ParseError>(Cow::Owned(element_id))
            })
            .transpose()?;
            (i, element_id)
        };

        check_last_json_field(&mut fields, i, SIGIL)?;
        Ok(Self {
            id,
            labels,
            properties: Cow::Owned(properties),
            element_id,
        })
    }
}

impl JoltNodeData<'_> {
    pub(super) fn jolt_fmt(
        &self,
        jolt_version_data: JoltVersion,
        jolt_version_ctx: JoltVersion,
    ) -> impl Display + '_ {
        struct JoltFormatter<'a> {
            data: &'a JoltNodeData<'a>,
            jolt_version_data: JoltVersion,
            jolt_version_ctx: JoltVersion,
        }

        impl Display for JoltFormatter<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                fmt_jolt_sigil_and_map(
                    f,
                    SIGIL,
                    self.jolt_version_data,
                    self.jolt_version_ctx,
                    Some(("[", "]")),
                    |f| {
                        Display::fmt(&self.data.id, f)?;
                        f.write_str(", [")?;
                        let mut labels = self.data.labels.iter();
                        if let Some(label) = labels.next() {
                            f.write_str("\"")?;
                            Display::fmt(label, f)?;
                            for label in labels {
                                f.write_str("\", \"")?;
                                Display::fmt(label, f)?;
                            }
                            f.write_str("\"")?;
                        }
                        f.write_str("], {")?;
                        let properties = self.data.properties.iter().map(|(k, v)| (k.as_str(), v));
                        write_joined_entries(f, properties, self.jolt_version_ctx)?;
                        match self.data.element_id.inner(self.jolt_version_data) {
                            None => f.write_str("}"),
                            Some(element_id) => {
                                f.write_str(r#"}, ""#)?;
                                Display::fmt(element_id, f)?;
                                f.write_str(r#"""#)
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

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct JoltNode<'a> {
    pub(crate) data: JoltNodeData<'a>,
    pub(super) jolt_version: JoltVersion,
}

impl JoltNode<'static> {
    pub(crate) fn parse(
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<Self, ParseError> {
        JoltNodeData::parse(v, jolt_version, config).map(|data| Self { data, jolt_version })
    }
}

impl<'a> JoltNode<'a> {
    pub(super) fn new(data: JoltNodeData<'a>, jolt_version: JoltVersion) -> Self {
        data.element_id.assert_jolt_version(jolt_version);
        Self { data, jolt_version }
    }

    pub(crate) fn into_struct(self) -> PackStreamStruct {
        let mut fields = Vec::with_capacity(4);
        fields.extend([
            PackStreamValue::Integer(self.data.id),
            PackStreamValue::List(
                self.data
                    .labels
                    .into_iter()
                    .map(|l| PackStreamValue::String(l.into_owned()))
                    .collect(),
            ),
            PackStreamValue::Dict(self.data.properties.into_owned()),
        ]);
        if let Some(element_id) = self.data.element_id.into_inner(self.jolt_version) {
            fields.push(PackStreamValue::String(element_id.into_owned()));
        }
        PackStreamStruct {
            tag: TAG_NODE,
            fields,
        }
    }

    pub(super) fn from_struct(s: &'a PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        if s.tag != TAG_NODE {
            return None;
        }
        let mut fields = s.fields.iter();
        let id = next_pack_stream_field(&mut fields)?;
        let labels: Vec<&String> = next_pack_stream_field(&mut fields)?;
        let labels = labels
            .into_iter()
            .map(String::as_str)
            .map(Cow::Borrowed)
            .collect();
        let properties = Cow::Borrowed(next_pack_stream_field(&mut fields)?);

        let element_id = ElementIdExt::new_lazy(jolt_version, || {
            Some(Cow::Borrowed(next_pack_stream_field(&mut fields)?))
        })
        .transpose()?;

        check_last_pack_stream_field(&mut fields).then_some(())?;
        Some(Self {
            data: JoltNodeData {
                id,
                labels,
                properties,
                element_id,
            },
            jolt_version,
        })
    }

    pub(super) fn jolt_fmt(&self, jolt_version: JoltVersion) -> impl Display + '_ {
        self.data.jolt_fmt(self.jolt_version, jolt_version)
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

    #[rstest]
    #[case(
        r#"[123, ["FOO", "BAR"], {"a": 1, "b": null}]"#,
        123,
        &["FOO", "BAR"],
        &[("a", 1.into()), ("b", None::<i64>.into())],
    )]
    #[case(
        "[-1, [], {}]",
        -1,
        &[],
        &[],
    )]
    #[case(
        r#"[0, ["FOO", "BAR", "", "BAZ"], {}]"#,
        0,
        &["FOO", "BAR", "", "BAZ"],
        &[],
    )]
    fn test_parse_v1(
        #[case] input: &str,
        #[case] id: i64,
        #[case] labels: &[&str],
        #[case] properties: &[(&str, PackStreamValue)],
    ) {
        let jolt_version = JoltVersion::V1;
        let config = make_actor_config(jolt_version);
        let input = serde_json::from_str(input).unwrap();
        let properties = properties
            .iter()
            .map(|(k, v)| ((*k).into(), v.clone()))
            .collect();

        let parsed = JoltNode::parse(input, jolt_version, &config).expect("case input failed");

        let data = JoltNodeData {
            id,
            labels: labels.iter().map(|l| (*l).into()).collect(),
            properties: Cow::Owned(properties),
            element_id: ElementIdExt::new(jolt_version, "".into()),
        };
        let expected = JoltNode { data, jolt_version };
        assert_eq!(parsed, expected);
    }

    #[rstest]
    #[case(
        r#"[123, ["FOO", "BAR"], {"a": 1, "b": null}, "abc"]"#,
        123,
        &["FOO", "BAR"],
        &[("a", 1.into()), ("b", None::<i64>.into())],
        "abc",
    )]
    #[case(
        r#"[-1, [], {}, ""]"#,
        -1,
        &[],
        &[],
        "",
    )]
    #[case(
        r#"[0, ["FOO", "BAR", "", "BAZ"], {}, "1"]"#,
        0,
        &["FOO", "BAR", "", "BAZ"],
        &[],
        "1",
    )]
    fn test_parse_v2_plus(
        #[case] input: &str,
        #[case] id: i64,
        #[case] labels: &[&str],
        #[case] properties: &[(&str, PackStreamValue)],
        #[case] element_id: &str,
        #[values(JoltVersion::V2, JoltVersion::V3, JoltVersion::V4)] jolt_version: JoltVersion,
    ) {
        let config = make_actor_config(jolt_version);
        let input = serde_json::from_str(input).unwrap();
        let properties = properties
            .iter()
            .map(|(k, v)| ((*k).into(), v.clone()))
            .collect();

        let parsed = JoltNode::parse(input, jolt_version, &config).expect("case input failed");

        let data = JoltNodeData {
            id,
            labels: labels.iter().map(|l| (*l).into()).collect(),
            properties: Cow::Owned(properties),
            element_id: ElementIdExt::new(jolt_version, element_id.into()),
        };
        let expected = JoltNode { data, jolt_version };
        assert_eq!(parsed, expected);
    }

    #[rstest]
    #[case(
        r#"["123", ["FOO", "BAR"], {"a": 1, "b": null}]"#,
        &["node id (field 1)", "to be an integer", "found string"],
    )]
    #[case(
        r#"[123, {"0": "FOO"}, {"a": 1, "b": null}]"#,
        &["labels (field 2)", "to be an array", "found object"],
    )]
    #[case(
        r#"[123, [1, 2], {"a": 1, "b": null}]"#,
        &["labels (field 2)", "to be an array of strings", "found number"],
    )]
    #[case(
        "[123, [], null]",
        &["properties (field 3)", "to be a map", "found null"],
    )]
    #[case(
        "[]",
        &["missing", "node id (field 1)"],
    )]
    #[case(
        "[123]",
        &["missing", "labels (field 2)"],
    )]
    #[case(
        "[123, []]",
        &["missing", "properties (field 3)"],
    )]
    #[case(
        r#"[123, ["FOO", "BAR"], {"a": 1, "b": null}, "abc"]"#,
        &["too many fields"],
    )]
    fn test_failing_parse_v1(#[case] input: &str, #[case] err_matchers: &[&str]) {
        let jolt_version = JoltVersion::V1;
        let config = make_actor_config(jolt_version);
        let input = serde_json::from_str(input).unwrap();

        let err = JoltNode::parse(input, jolt_version, &config)
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
        r#"["123", ["FOO", "BAR"], {"a": 1, "b": null}, "abc"]"#,
        &["node id (field 1)", "to be an integer", "found string"],
    )]
    #[case(
        r#"[123, {"0": "FOO"}, {"a": 1, "b": null}, "abc"]"#,
        &["labels (field 2)", "to be an array", "found object"],
    )]
    #[case(
        r#"[123, [1, 2], {"a": 1, "b": null}, "abc"]"#,
        &["labels (field 2)", "to be an array of strings", "found number"],
    )]
    #[case(
        r#"[123, [], null, "abc"]"#,
        &["properties (field 3)", "to be a map", "found null"],
    )]
    #[case(
        "[123, [], {}, null]",
        &["element id (field 4)", "to be a string", "found null"],
    )]
    #[case(
        "[]",
        &["missing", "node id (field 1)"],
    )]
    #[case(
        "[123]",
        &["missing", "labels (field 2)"],
    )]
    #[case(
        "[123, []]",
        &["missing", "properties (field 3)"],
    )]
    #[case(
        "[123, [], {}]",
        &["missing", "element id (field 4)"],
    )]
    #[case(
        r#"[123, [], {}, "abc", null]"#,
        &["too many fields"],
    )]
    fn test_failing_parse_v2_plus(
        #[case] input: &str,
        #[case] err_matchers: &[&str],
        #[values(JoltVersion::V2, JoltVersion::V3, JoltVersion::V4)] jolt_version: JoltVersion,
    ) {
        let config = make_actor_config(jolt_version);
        let input = serde_json::from_str(input).unwrap();

        let err = JoltNode::parse(input, jolt_version, &config)
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
        r#"[0, [], {"uuid": {"UU": "12345678-1234-1234-1234-123456789012"}}]"#,
        JoltVersion::V1,
        false,
        false
    )]
    #[case(
        r#"[0, [], {"uuid": {"UU": "12345678-1234-1234-1234-123456789012"}}, ""]"#,
        JoltVersion::V4,
        false,
        true
    )]
    #[case(
        r#"[0, [], {"uuid": {"UUv4": "12345678-1234-1234-1234-123456789012"}}]"#,
        JoltVersion::V1,
        true,
        true
    )]
    #[case(
        r#"[0, [], {"uuid": {"UUv1": "12345678-1234-1234-1234-123456789012"}}, ""]"#,
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

        let res = JoltNode::parse(input, jolt_version, &config);
        if fails {
            res.expect_err("didn't fail parsing");
            return;
        }
        let JoltNode {
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
        r#"[123, ["FOO", "BAR"], {"a": 1, "b": null}]"#,
        123,
        &["FOO", "BAR"],
        &[("a", 1.into()), ("b", None::<i64>.into())],
    )]
    #[case(
        "[-1, [], {}]",
        -1,
        &[],
        &[],
    )]
    #[case(
        r#"[0, ["FOO", "BAR", "", "BAZ"], {}]"#,
        0,
        &["FOO", "BAR", "", "BAZ"],
        &[],
    )]
    fn test_jolt_fmt_v1(
        #[case] expected: &str,
        #[case] id: i64,
        #[case] labels: &[&str],
        #[case] properties: &[(&str, PackStreamValue)],
    ) {
        let jolt_version = JoltVersion::V1;
        let properties = properties
            .iter()
            .map(|(k, v)| ((*k).into(), v.clone()))
            .collect();
        let data = JoltNodeData {
            id,
            labels: labels.iter().map(|l| (*l).into()).collect(),
            properties: Cow::Owned(properties),
            element_id: ElementIdExt::new(jolt_version, "".into()),
        };
        let node = JoltNode { data, jolt_version };

        let expected = format!(r#"{{"()": {expected}}}"#);
        assert_eq!(node.jolt_fmt(jolt_version).to_string(), expected);
    }

    #[rstest]
    #[case(
        r#"[123, ["FOO", "BAR"], {"a": 1, "b": null}, "abc"]"#,
        123,
        &["FOO", "BAR"],
        &[("a", 1.into()), ("b", None::<i64>.into())],
        "abc",
    )]
    #[case(
        r#"[-1, [], {}, ""]"#,
        -1,
        &[],
        &[],
        "",
    )]
    #[case(
        r#"[0, ["FOO", "BAR", "", "BAZ"], {}, "1"]"#,
        0,
        &["FOO", "BAR", "", "BAZ"],
        &[],
        "1",
    )]
    fn test_jolt_fmt_v2_plus(
        #[case] expected: &str,
        #[case] id: i64,
        #[case] labels: &[&str],
        #[case] properties: &[(&str, PackStreamValue)],
        #[case] element_id: &str,
        #[values(JoltVersion::V2, JoltVersion::V3, JoltVersion::V4)] jolt_version: JoltVersion,
    ) {
        let properties = properties
            .iter()
            .map(|(k, v)| ((*k).into(), v.clone()))
            .collect();
        let data = JoltNodeData {
            id,
            labels: labels.iter().map(|l| (*l).into()).collect(),
            properties: Cow::Owned(properties),
            element_id: ElementIdExt::new(jolt_version, element_id.into()),
        };
        let node = JoltNode { data, jolt_version };

        let expected = format!(r#"{{"()": {expected}}}"#);
        assert_eq!(node.jolt_fmt(jolt_version).to_string(), expected);
    }

    #[rstest]
    #[case(
        0,
        &[],
        &[],
        "",
    )]
    #[case(
        i64::MAX,
        &["", "FOO", "BAR"],
        &[("a", None::<i64>.into()), ("c", 1.into()), ("b", vec![1, 2, 3].into())],
        "abc",
    )]
    #[case(
        i64::MIN,
        &["🍨"],
        &[("a", None::<i64>.into()), ("c", 1.into()), ("b", vec![1, 2, 3].into())],
        "🍨",
    )]
    fn test_as_struct(
        #[case] id: i64,
        #[case] labels: &[&str],
        #[case] properties: &[(&str, PackStreamValue)],
        #[case] element_id: &str,
        #[values(JoltVersion::V1, JoltVersion::V2, JoltVersion::V3, JoltVersion::V4)]
        jolt_version: JoltVersion,
    ) {
        let properties: IndexMap<String, PackStreamValue> = properties
            .iter()
            .map(|(k, v)| ((*k).into(), v.clone()))
            .collect();
        let data = JoltNodeData {
            id,
            labels: labels.iter().map(|l| (*l).into()).collect(),
            properties: Cow::Owned(properties.clone()),
            element_id: ElementIdExt::new(jolt_version, element_id.into()),
        };
        let node = JoltNode { data, jolt_version };

        let struct_ = node.into_struct();

        let mut fields = vec![
            id.into(),
            PackStreamValue::List(labels.iter().map(|l| (*l).into()).collect()),
            properties.into(),
        ];
        if ElementIdExt::uses_element_id(jolt_version) {
            fields.push(element_id.into());
        }
        let expected = PackStreamStruct {
            tag: TAG_NODE,
            fields,
        };
        assert_eq!(struct_, expected);
    }

    #[rstest]
    #[case(
        0,
        &[],
        &[],
        "",
    )]
    #[case(
        i64::MAX,
        &["", "FOO", "BAR"],
        &[("a", None::<i64>.into()), ("c", 1.into()), ("b", vec![1, 2, 3].into())],
        "abc",
    )]
    #[case(
        i64::MIN,
        &["🍨"],
        &[("a", None::<i64>.into()), ("c", 1.into()), ("b", vec![1, 2, 3].into())],
        "🍨",
    )]
    fn test_from_struct(
        #[case] id: i64,
        #[case] labels: &[&str],
        #[case] properties: &[(&str, PackStreamValue)],
        #[case] element_id: &str,
        #[values(JoltVersion::V1, JoltVersion::V2, JoltVersion::V3, JoltVersion::V4)]
        jolt_version: JoltVersion,
    ) {
        let properties: IndexMap<String, PackStreamValue> = properties
            .iter()
            .map(|(k, v)| ((*k).into(), v.clone()))
            .collect();
        let mut fields = vec![
            id.into(),
            PackStreamValue::List(labels.iter().map(|l| (*l).into()).collect()),
            properties.clone().into(),
        ];
        if ElementIdExt::uses_element_id(jolt_version) {
            fields.push(element_id.into());
        }
        let struct_ = PackStreamStruct {
            tag: TAG_NODE,
            fields,
        };

        let struct_ = JoltNode::from_struct(&struct_, jolt_version).expect("failed to load");

        let data = JoltNodeData {
            id,
            labels: labels.iter().map(|l| (*l).into()).collect(),
            properties: Cow::Owned(properties),
            element_id: ElementIdExt::new(jolt_version, element_id.into()),
        };
        let expected = JoltNode { data, jolt_version };

        assert_eq!(struct_, expected);
    }
}
