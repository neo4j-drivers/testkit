use std::borrow::Cow;
use std::fmt::{Debug, Display, Formatter};
use std::mem;

use indexmap::IndexMap;
use serde_json::Value as JsonValue;

use super::_parsing::{
    check_last_json_field, check_last_pack_stream_field, next_json_field, next_pack_stream_field,
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
        let mut fields = fields.into_iter().enumerate();
        let i = 0;

        let (i, id) = next_json_field(&mut fields, "id", i, SIGIL, config)?;
        let (i, start_node_id) = next_json_field(&mut fields, "start node id", i, SIGIL, config)?;
        let (i, rel_type) = next_json_field(&mut fields, "relation ship type", i, SIGIL, config)?;
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
                        f.write_str(", ")?;
                        Debug::fmt(&self.data.rel_type, f)?;
                        f.write_str(", ")?;
                        Display::fmt(&self.data.end_node_id, f)?;
                        f.write_str(", {")?;
                        let properties = self.data.properties.iter().map(|(k, v)| (k.as_str(), v));
                        write_joined_entries(f, properties, self.jolt_version_ctx)?;
                        match self.data.element_id_ext.inner(self.jolt_version_data) {
                            None => f.write_str("}"),
                            Some(ext) => {
                                f.write_str("}, ")?;
                                Debug::fmt(&ext.element_id, f)?;
                                f.write_str(", ")?;
                                Debug::fmt(&ext.start_node_element_id, f)?;
                                f.write_str(", ")?;
                                Debug::fmt(&ext.end_node_element_id, f)
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
