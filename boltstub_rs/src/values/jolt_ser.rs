use indexmap::IndexMap;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::Serialize;
use uuid::Uuid;

use crate::bolt_version::JoltVersion;
use crate::jolt::JoltSigil;
use crate::str_bytes::fmt_bytes;
use crate::values::bolt_struct::{BoltStruct, JoltStruct};
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

#[derive(Debug, Clone, Copy)]
pub struct JoltSer<'a> {
    value: JoltSerValue<'a>,
    jolt_version: JoltVersion,
}

impl<'a> JoltSer<'a> {
    pub fn new(value: &'a PackStreamValue, jolt_version: JoltVersion) -> Self {
        Self {
            value: JoltSerValue::from(value),
            jolt_version,
        }
    }

    pub fn new_value(value: JoltSerValue<'a>, jolt_version: JoltVersion) -> Self {
        Self {
            value,
            jolt_version,
        }
    }

    fn child<'b>(&self, value: &'b PackStreamValue) -> JoltSer<'b> {
        JoltSer {
            value: JoltSerValue::from(value),
            ..*self
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum JoltSerValue<'a> {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Bytes(&'a [u8]),
    String(&'a str),
    List(&'a [PackStreamValue]),
    Dict(&'a IndexMap<String, PackStreamValue>),
    Uuid(Uuid),
    Struct(&'a PackStreamStruct),
}

impl<'a> From<&'a PackStreamValue> for JoltSerValue<'a> {
    fn from(value: &'a PackStreamValue) -> Self {
        match value {
            PackStreamValue::Null => JoltSerValue::Null,
            PackStreamValue::Boolean(v) => JoltSerValue::Boolean(*v),
            PackStreamValue::Integer(v) => JoltSerValue::Integer(*v),
            PackStreamValue::Float(v) => JoltSerValue::Float(*v),
            PackStreamValue::Bytes(v) => JoltSerValue::Bytes(v),
            PackStreamValue::String(v) => JoltSerValue::String(v),
            PackStreamValue::List(v) => JoltSerValue::List(v),
            PackStreamValue::Dict(index_map) => JoltSerValue::Dict(index_map),
            PackStreamValue::Uuid(uuid) => JoltSerValue::Uuid(*uuid),
            PackStreamValue::Struct(pack_stream_struct) => JoltSerValue::Struct(pack_stream_struct),
        }
    }
}

impl Serialize for JoltSer<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self.value {
            JoltSerValue::Null => serializer.serialize_none(),
            JoltSerValue::Boolean(v) => serializer.serialize_bool(*v),
            JoltSerValue::Integer(v) => serializer.serialize_i64(*v),
            JoltSerValue::Float(v) => serializer.serialize_f64(*v),
            JoltSerValue::Bytes(v) => Self::serialize_bytes(serializer, v),
            JoltSerValue::String(v) => serializer.serialize_str(v),
            JoltSerValue::List(v) => self.serialize_list(serializer, v),
            JoltSerValue::Dict(v) => self.serialize_dict(serializer, v),
            JoltSerValue::Uuid(v) => self.serialize_uuid(serializer, *v),
            JoltSerValue::Struct(v) => self.serialize_struct(serializer, v),
        }
    }
}

impl<'a> JoltSer<'a> {
    fn serialize_bytes<S>(serializer: S, bytes: &[u8]) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("#", &BytesSer(bytes))?;
        map.end()
    }

    fn serialize_list<S>(&self, serializer: S, list: &[PackStreamValue]) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(list.len()))?;
        for item in list {
            seq.serialize_element(&self.child(item))?;
        }
        seq.end()
    }

    fn serialize_dict<S>(
        &self,
        serializer: S,
        dict: &'a IndexMap<String, PackStreamValue>,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if dict
            .keys()
            .next()
            .and_then(|k| JoltSigil::from_str(k, self.jolt_version))
            .is_some_and(|_| dict.len() == 1)
        {
            // ambiguous: Dict looks like Jolt type
            let mut map = serializer.serialize_map(Some(1))?;
            map.serialize_entry("{}", &EscapedMap::new(dict, self.jolt_version))?;
            map.end()
        } else {
            EscapedMap::new(dict, self.jolt_version).serialize(serializer)
        }
    }

    fn serialize_uuid<S>(&self, serializer: S, uuid: Uuid) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let key = match self.jolt_version {
            JoltVersion::V1 | JoltVersion::V2 | JoltVersion::V3 => "UUv4",
            JoltVersion::V4 => "UU",
        };
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(key, &UuidSer(uuid))?;
        map.end()
    }

    fn serialize_struct<S>(
        &self,
        serializer: S,
        struct_: &PackStreamStruct,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let parsed_struct = BoltStruct::read(struct_, self.jolt_version);
        if let Some(bolt_struct) = parsed_struct {
            return bolt_struct.jolt_serialize(serializer, self.jolt_version);
        }
        let parsed_struct = BoltStruct::read_other_jolt_version(struct_, self.jolt_version);
        if let Some((bolt_struct, _jolt_version)) = parsed_struct {
            return bolt_struct.jolt_serialize(serializer, self.jolt_version);
        }
        let bolt_struct = JoltStruct::from_struct(struct_, self.jolt_version);
        bolt_struct.jolt_serialize(serializer, self.jolt_version)
    }
}

#[derive(Debug, Clone, Copy)]
struct BytesSer<'a>(&'a [u8]);

impl Serialize for BytesSer<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(&fmt_bytes(self.0))
    }
}

#[derive(Debug, Clone, Copy)]
struct EscapedMap<'a> {
    map: &'a IndexMap<String, PackStreamValue>,
    jolt_version: JoltVersion,
}

impl<'a> EscapedMap<'a> {
    fn new(map: &'a IndexMap<String, PackStreamValue>, jolt_version: JoltVersion) -> Self {
        Self { map, jolt_version }
    }
}

impl Serialize for EscapedMap<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.map.len()))?;
        for (k, v) in self.map {
            map.serialize_entry(k, &JoltSer::new(v, self.jolt_version))?;
        }
        map.end()
    }
}

#[derive(Debug, Clone, Copy)]
struct UuidSer(Uuid);

impl Serialize for UuidSer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct JoltSigilMapSer<'a, B> {
    sigil: &'a str,
    jolt_version_inner: JoltVersion,
    jolt_version_context: JoltVersion,
    body: &'a B,
}

impl<'a, B> JoltSigilMapSer<'a, B>
where
    B: serde::Serialize,
{
    pub fn new(
        sigil: &'a str,
        jolt_version_inner: JoltVersion,
        jolt_version_context: JoltVersion,
        body: &'a B,
    ) -> Self {
        Self {
            sigil,
            jolt_version_inner,
            jolt_version_context,
            body,
        }
    }
}

impl<B> serde::Serialize for JoltSigilMapSer<'_, B>
where
    B: serde::Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let key = JoltSigilSer {
            sigil: self.sigil,
            jolt_version_inner: self.jolt_version_inner,
            jolt_version_context: self.jolt_version_context,
        };
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(&key, self.body)?;
        map.end()
    }
}

#[derive(Debug, Clone, Copy)]
struct JoltSigilSer<'a> {
    sigil: &'a str,
    jolt_version_inner: JoltVersion,
    jolt_version_context: JoltVersion,
}

impl std::fmt::Display for JoltSigilSer<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let with_postfix = self.jolt_version_inner != self.jolt_version_context;

        f.write_str(self.sigil)?;
        if with_postfix {
            f.write_str("v")?;
            f.write_str(self.jolt_version_inner.to_str())?;
        }
        Ok(())
    }
}

impl serde::Serialize for JoltSigilSer<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(&self)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::ext::serde_json::support_pub::JoltSerializer;
    use crate::values::tests::jolt_serializer;

    use super::*;

    #[rstest]
    #[case(
        JoltSigilMapSer::new("SigIL💯", JoltVersion::V1, JoltVersion::V1, &""),
        r#"{"SigIL💯": ""}"#
    )]
    #[case(
        JoltSigilMapSer::new("T", JoltVersion::V1, JoltVersion::V2, &1),
        r#"{"Tv1": 1}"#
    )]
    #[case(
        JoltSigilMapSer::new("", JoltVersion::V2, JoltVersion::V1, &["a", "b"]),
        r#"{"v2": ["a", "b"]}"#
    )]
    #[case(
        JoltSigilMapSer::new("\"\\", JoltVersion::V3, JoltVersion::V2, &(1, 2)),
        r#"{"\"\\v3": [1, 2]}"#
    )]
    fn test_jolt_sigil_map_ser<B: serde::Serialize>(
        #[case] map: JoltSigilMapSer<'_, B>,
        #[case] expected: &str,
        mut jolt_serializer: JoltSerializer<Vec<u8>>,
    ) {
        map.serialize(jolt_serializer.ser())
            .expect("Failed to serialize Jolt");
        let formatted = jolt_serializer.into_string();

        assert_eq!(formatted, expected);
    }
}
