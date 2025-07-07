use std::fmt::{Debug, Display, Formatter};
use std::mem;

use indexmap::IndexMap;
use serde_json::Value as JsonValue;

use super::_parsing::{
    check_last_json_field, check_last_pack_stream_field, next_json_field, next_pack_stream_field,
};
use crate::bolt_version::JoltVersion;
use crate::parse_error::ParseError;
use crate::parser::ActorConfig;
use crate::values::bolt_struct::{TAG_RELATIONSHIP, TAG_UNBOUND_RELATIONSHIP};
use crate::values::pack_stream_value::{write_joined_entries, PackStreamStruct, PackStreamValue};

#[derive(Debug, Clone, Eq)]
pub(crate) struct JoltRelationship {
    pub(crate) id: i64,
    pub(crate) start_node_id: i64,
    pub(crate) rel_type: String,
    pub(crate) end_node_id: i64,
    pub(crate) properties: IndexMap<String, PackStreamValue>,
    pub(crate) element_id_ext: Option<JoltRelationshipElementIdExt>,
}

impl PartialEq for JoltRelationship {
    fn eq(&self, other: &Self) -> bool {
        BoltRelationship::from_jolt_relationship(self)
            == BoltRelationship::from_jolt_relationship(other)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct JoltRelationshipElementIdExt {
    pub(crate) element_id: String,
    pub(crate) start_node_element_id: String,
    pub(crate) end_node_element_id: String,
}

impl JoltRelationship {
    pub(crate) fn parse(
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<Self, ParseError> {
        let JsonValue::Array(fields) = v else {
            return Err(ParseError::new(format!(
                "Expected array after sigil \"{{}}\", but found {v:?}"
            )));
        };
        let mut fields = fields.into_iter().enumerate();
        let i = 0;

        let (i, id) = next_json_field(&mut fields, "id", i, "{}", config)?;
        let (i, start_node_id) = next_json_field(&mut fields, "start node id", i, "{}", config)?;
        let (i, rel_type) = next_json_field(&mut fields, "relation ship type", i, "{}", config)?;
        let (i, end_node_id) = next_json_field(&mut fields, "end node id", i, "{}", config)?;
        let (i, properties) = next_json_field(&mut fields, "properties", i, "{}", config)?;
        let (i, element_id_ext) = match jolt_version {
            JoltVersion::V1 => (i, None),
            JoltVersion::V2 => {
                let (i, element_id) = next_json_field(&mut fields, "element id", i, "{}", config)?;
                let (i, start_node_element_id) =
                    next_json_field(&mut fields, "start node element id", i, "{}", config)?;
                let (i, end_node_element_id) =
                    next_json_field(&mut fields, "end node element id", i, "{}", config)?;
                (
                    i,
                    Some(JoltRelationshipElementIdExt {
                        element_id,
                        start_node_element_id,
                        end_node_element_id,
                    }),
                )
            }
        };
        check_last_json_field(&mut fields, i, "{}")?;

        Ok(Self {
            id,
            start_node_id,
            rel_type,
            end_node_id,
            properties,
            element_id_ext,
        })
    }

    pub(crate) fn flip_direction(&mut self) {
        mem::swap(&mut self.start_node_id, &mut self.end_node_id);
        if let Some(ext) = self.element_id_ext.as_mut() {
            mem::swap(&mut ext.start_node_element_id, &mut ext.end_node_element_id);
        }
    }

    pub(crate) fn into_struct(self) -> PackStreamStruct {
        let mut fields = Vec::with_capacity(8);
        fields.extend([
            PackStreamValue::Integer(self.id),
            PackStreamValue::Integer(self.start_node_id),
            PackStreamValue::Integer(self.end_node_id),
            PackStreamValue::String(self.rel_type),
            PackStreamValue::Dict(self.properties),
        ]);
        if let Some(element_id_ext) = self.element_id_ext {
            fields.push(PackStreamValue::String(element_id_ext.element_id));
            fields.push(PackStreamValue::String(
                element_id_ext.start_node_element_id,
            ));
            fields.push(PackStreamValue::String(element_id_ext.end_node_element_id));
        }
        PackStreamStruct {
            tag: TAG_RELATIONSHIP,
            fields,
        }
    }
}

#[derive(Debug, Clone, Eq)]
pub(crate) struct BoltRelationship<'a> {
    pub(crate) id: i64,
    pub(crate) start_node_id: i64,
    pub(crate) rel_type: &'a str,
    pub(crate) end_node_id: i64,
    pub(crate) properties: &'a IndexMap<String, PackStreamValue>,
    pub(crate) element_id_ext: Option<BoltRelationshipElementIdExt<'a>>,
}

impl PartialEq for BoltRelationship<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.start_node_id == other.start_node_id
            && self.rel_type == other.rel_type
            && self.end_node_id == other.end_node_id
            && self.element_id_ext == other.element_id_ext
            && self.properties == other.properties
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) struct BoltRelationshipElementIdExt<'a> {
    pub(super) element_id: &'a str,
    pub(super) start_node_element_id: &'a str,
    pub(super) end_node_element_id: &'a str,
}

impl<'a> BoltRelationship<'a> {
    fn from_jolt_relationship(rel: &'a JoltRelationship) -> Self {
        Self {
            id: rel.id,
            start_node_id: rel.start_node_id,
            rel_type: &rel.rel_type,
            end_node_id: rel.end_node_id,
            properties: &rel.properties,
            element_id_ext: rel
                .element_id_ext
                .as_ref()
                .map(|ext| BoltRelationshipElementIdExt {
                    element_id: &ext.element_id,
                    start_node_element_id: &ext.start_node_element_id,
                    end_node_element_id: &ext.end_node_element_id,
                }),
        }
    }

    pub(crate) fn flip_direction(&mut self) {
        mem::swap(&mut self.start_node_id, &mut self.end_node_id);
        if let Some(ext) = self.element_id_ext.as_mut() {
            mem::swap(&mut ext.start_node_element_id, &mut ext.end_node_element_id);
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
        let rel_type = next_pack_stream_field(&mut fields)?;
        let properties = next_pack_stream_field(&mut fields)?;
        let element_id_ext = {
            match jolt_version {
                JoltVersion::V1 => None,
                JoltVersion::V2 => {
                    let element_id = next_pack_stream_field(&mut fields)?;
                    let start_node_element_id = next_pack_stream_field(&mut fields)?;
                    let end_node_element_id = next_pack_stream_field(&mut fields)?;
                    Some(BoltRelationshipElementIdExt {
                        element_id,
                        start_node_element_id,
                        end_node_element_id,
                    })
                }
            }
        };
        if !check_last_pack_stream_field(&mut fields) {
            return None;
        }
        Some(Self {
            id,
            start_node_id,
            rel_type,
            end_node_id,
            properties,
            element_id_ext,
        })
    }

    pub(super) fn jolt_fmt(&self, jolt_version: JoltVersion) -> impl Display + '_ {
        struct JoltFormatter<'a> {
            this: &'a BoltRelationship<'a>,
            jolt_version: JoltVersion,
        }

        impl Display for JoltFormatter<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.write_str(r#"{"->": ["#)?;
                Display::fmt(&self.this.id, f)?;
                f.write_str(", ")?;
                Display::fmt(&self.this.start_node_id, f)?;
                f.write_str(", ")?;
                Debug::fmt(&self.this.rel_type, f)?;
                f.write_str(", ")?;
                Display::fmt(&self.this.end_node_id, f)?;
                f.write_str(", {")?;
                write_joined_entries(f, self.this.properties.iter(), self.jolt_version)?;
                match self.this.element_id_ext.as_ref() {
                    None => f.write_str(r#"}]}"#),
                    Some(element_id_ext) => {
                        f.write_str("}, ")?;
                        Debug::fmt(&element_id_ext.element_id, f)?;
                        f.write_str(", ")?;
                        Debug::fmt(&element_id_ext.start_node_element_id, f)?;
                        f.write_str(", ")?;
                        Debug::fmt(&element_id_ext.end_node_element_id, f)?;
                        f.write_str(r#"]}"#)
                    }
                }
            }
        }

        JoltFormatter {
            this: self,
            jolt_version,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct BoltUnboundRelationship<'a> {
    pub(super) id: i64,
    pub(super) rel_type: &'a str,
    pub(super) properties: &'a IndexMap<String, PackStreamValue>,
    pub(super) element_id: Option<&'a str>,
}

impl<'a> BoltUnboundRelationship<'a> {
    pub(super) fn from_struct(s: &'a PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        if s.tag != TAG_UNBOUND_RELATIONSHIP {
            return None;
        }
        let mut fields = s.fields.iter();
        let id = next_pack_stream_field(&mut fields)?;
        let rel_type = next_pack_stream_field(&mut fields)?;
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
            rel_type,
            properties,
            element_id,
        })
    }
}
