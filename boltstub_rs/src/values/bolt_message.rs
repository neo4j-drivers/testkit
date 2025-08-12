use std::borrow::Cow;

use itertools::Itertools;

use crate::bolt_version::BoltVersion;
use crate::values::pack_stream_value::{value_jolt_fmt, PackStreamStruct, PackStreamValue};

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
            let jolt_version = self.bolt_version.jolt_version();
            let body = self
                .fields
                .iter()
                .map(|v| value_jolt_fmt(v, jolt_version))
                .join(" ");
            format!("{name} {body}")
        }
    }

    pub fn into_data(self) -> Vec<u8> {
        PackStreamValue::Struct(PackStreamStruct {
            tag: self.tag,
            fields: self.fields,
        })
        .as_data()
    }
}
