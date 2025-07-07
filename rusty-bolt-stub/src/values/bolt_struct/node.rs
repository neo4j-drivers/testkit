use std::fmt::{Debug, Display, Formatter};

use indexmap::IndexMap;
use itertools::Itertools;
use serde_json::Value as JsonValue;

use super::_parsing::{
    check_last_json_field, check_last_pack_stream_field, next_json_field, next_pack_stream_field,
};
use crate::bolt_version::JoltVersion;
use crate::parse_error::ParseError;
use crate::parser::ActorConfig;
use crate::values::bolt_struct::TAG_NODE;
use crate::values::pack_stream_value::{write_joined_entries, PackStreamStruct, PackStreamValue};

#[derive(Debug, Clone, Eq)]
pub(crate) struct JoltNode {
    pub(crate) id: i64,
    pub(crate) labels: Vec<String>,
    pub(crate) properties: IndexMap<String, PackStreamValue>,
    pub(crate) element_id: Option<String>,
}

impl PartialEq for JoltNode {
    fn eq(&self, other: &Self) -> bool {
        BoltNode::from_jolt_node(self) == BoltNode::from_jolt_node(other)
    }
}

impl JoltNode {
    pub(crate) fn parse(
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<Self, ParseError> {
        let JsonValue::Array(fields) = v else {
            return Err(ParseError::new(format!(
                "Expected array after sigil \"()\", but found {v:?}"
            )));
        };
        let mut fields = fields.into_iter().enumerate();
        let i = 0;

        let (i, id) = next_json_field(&mut fields, "node id", i, "()", config)?;
        let (i, labels) = next_json_field::<Vec<String>>(&mut fields, "labels", i, "()", config)?;
        let labels = labels.into_iter().collect();
        let (i, properties) = next_json_field(&mut fields, "properties", i, "()", config)?;
        let (i, element_id) = match jolt_version {
            JoltVersion::V1 => (i, None),
            JoltVersion::V2 => {
                let (i, element_id) = next_json_field(&mut fields, "element id", i, "()", config)?;
                (i, Some(element_id))
            }
        };
        check_last_json_field(&mut fields, i, "()")?;

        Ok(Self {
            id,
            labels,
            properties,
            element_id,
        })
    }

    pub(crate) fn into_struct(self) -> PackStreamStruct {
        let mut fields = Vec::with_capacity(4);
        fields.extend([
            PackStreamValue::Integer(self.id),
            PackStreamValue::List(
                self.labels
                    .into_iter()
                    .map(PackStreamValue::String)
                    .collect(),
            ),
            PackStreamValue::Dict(self.properties),
        ]);
        if let Some(element_id) = self.element_id {
            fields.push(PackStreamValue::String(element_id));
        }
        PackStreamStruct {
            tag: TAG_NODE,
            fields,
        }
    }
}
#[derive(Debug, Clone, Eq)]
pub(super) struct BoltNode<'a> {
    pub(super) id: i64,
    pub(super) labels: Vec<&'a str>,
    pub(super) properties: &'a IndexMap<String, PackStreamValue>,
    pub(super) element_id: Option<&'a str>,
}

impl PartialEq for BoltNode<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.properties == other.properties
            && self.element_id == other.element_id
            && self.labels.len() == other.labels.len()
            && self.labels.iter().sorted().eq(other.labels.iter().sorted())
    }
}
impl<'a> BoltNode<'a> {
    fn from_jolt_node(jolt_node: &'a JoltNode) -> Self {
        Self {
            id: jolt_node.id,
            labels: jolt_node.labels.iter().map(String::as_str).collect(),
            properties: &jolt_node.properties,
            element_id: jolt_node.element_id.as_deref(),
        }
    }

    pub(super) fn from_struct(s: &'a PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        if s.tag != TAG_NODE {
            return None;
        }
        let mut fields = s.fields.iter();
        let id = next_pack_stream_field(&mut fields)?;
        let labels = next_pack_stream_field(&mut fields)?;
        let properties = next_pack_stream_field(&mut fields)?;
        let element_id = match jolt_version {
            JoltVersion::V1 => None,
            JoltVersion::V2 => Some(next_pack_stream_field(&mut fields)?),
        };

        if !check_last_pack_stream_field(&mut fields) {
            return None;
        }
        Some(Self {
            id,
            labels,
            properties,
            element_id,
        })
    }

    pub(super) fn jolt_fmt(&self, jolt_version: JoltVersion) -> impl Display + '_ {
        struct JoltFormatter<'a> {
            this: &'a BoltNode<'a>,
            jolt_version: JoltVersion,
        }

        impl Display for JoltFormatter<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.write_str(r#"{"{}": ["#)?;
                Display::fmt(&self.this.id, f)?;

                f.write_str(", [")?;
                let mut labels = self.this.labels.iter();
                if let Some(label) = labels.next() {
                    Debug::fmt(label, f)?;
                    for label in labels {
                        f.write_str(", ")?;
                        Debug::fmt(label, f)?;
                    }
                }

                f.write_str("], {")?;
                write_joined_entries(f, self.this.properties.iter(), self.jolt_version)?;

                f.write_str(r#"}]}"#)
            }
        }

        JoltFormatter {
            this: self,
            jolt_version,
        }
    }
}
