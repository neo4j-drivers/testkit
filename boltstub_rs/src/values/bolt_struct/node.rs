use std::borrow::Cow;
use std::fmt::{Debug, Display, Formatter};

use indexmap::IndexMap;
use itertools::Itertools;
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
        let mut fields = fields.into_iter().enumerate();
        let i = 0;

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
                        let mut labels = self.data.labels.iter();
                        if let Some(label) = labels.next() {
                            Debug::fmt(label, f)?;
                            for label in labels {
                                f.write_str(", ")?;
                                Debug::fmt(label, f)?;
                            }
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
