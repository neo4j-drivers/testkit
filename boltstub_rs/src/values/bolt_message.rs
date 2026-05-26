use std::borrow::Cow;

use anyhow::Result;
use log::warn;

use crate::bolt_version::{BoltVersion, JoltVersion};
use crate::ext::serde_json as serde_json_ext;
use crate::values::jolt_ser::JoltSer;
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

#[derive(Debug, Clone)]
pub struct BoltMessage {
    pub tag: u8,
    pub fields: Vec<PackStreamValue>,
    pub bolt_version: BoltVersion,
}

impl BoltMessage {
    pub fn new(tag: u8, fields: Vec<PackStreamValue>, bolt_version: BoltVersion) -> Self {
        Self {
            tag,
            fields,
            bolt_version,
        }
    }

    pub fn repr(&self) -> String {
        let name: Cow<str> = match self.bolt_version.message_name_from_tag(self.tag) {
            None => format!("UNKNOWN[{:#04X}]", self.tag).into(),
            Some(name) => name.into(),
        };
        if self.fields.is_empty() {
            name.into_owned()
        } else {
            let body = self.try_repr_body().unwrap_or_else(|| {
                warn!("Failed to produce compact JSON repr for BoltMessage fields, falling back to debug repr");
                format!("{:?}", self.fields)
            });
            format!("{name} {body}")
        }
    }

    fn try_repr_body(&self) -> Option<String> {
        fn repr_field(
            buf: &mut Vec<u8>,
            field: &PackStreamValue,
            jolt_version: JoltVersion,
        ) -> Option<()> {
            let field_ser = JoltSer::new(field, jolt_version);
            serde_json_ext::to_writer_pretty_compact(buf, &field_ser)
                .inspect_err(|e| {
                    warn!("Failed to serialize read bolt message field: {field:?}: {e}");
                })
                .ok()
        }

        let mut buf = Vec::with_capacity(128);
        let mut fields = self.fields.iter();

        if let Some(first) = fields.next() {
            repr_field(&mut buf, first, self.bolt_version.jolt_version())?;
            for field in fields {
                buf.extend_from_slice(b" ");
                repr_field(&mut buf, field, self.bolt_version.jolt_version())?;
            }
        }

        String::from_utf8(buf)
            .inspect_err(|e| warn!("JoltSer produced invalid utf-8: {e}"))
            .ok()
    }

    pub fn into_serialized(self) -> Result<SerializedBoltMessage> {
        SerializedBoltMessage::new(self)
    }
}

#[derive(Debug, Clone)]
pub struct SerializedBoltMessage {
    pub message: BoltMessage,
    pub data: Vec<u8>,
}

impl SerializedBoltMessage {
    pub fn new(message: BoltMessage) -> Result<Self> {
        let data = PackStreamValue::Struct(PackStreamStruct {
            tag: message.tag,
            fields: message.fields.clone(),
        })
        .as_data(message.bolt_version.packstream_version())?;
        Ok(Self { message, data })
    }
}
