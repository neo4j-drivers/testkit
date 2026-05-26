mod _common;
mod _parsing;
mod date;
mod date_time;
mod duration;
mod node;
mod path;
mod point;
mod relationship;
mod struct_;
mod time;
mod unsupported_type;
mod vector;

use std::fmt::Debug;

pub(crate) use date::JoltDate;
pub(crate) use date_time::JoltDateTime;
pub(crate) use duration::{JoltDuration, JoltDurationData};
pub(crate) use node::JoltNode;
pub(crate) use path::JoltPath;
pub(crate) use point::JoltPoint;
pub(crate) use relationship::JoltRelationship;
pub(crate) use struct_::JoltStruct;
pub(crate) use time::JoltTime;
pub(crate) use unsupported_type::JoltUnsupportedType;
pub(crate) use vector::JoltVector;
pub(crate) use vector::JoltVectorType;

use crate::bolt_version::JoltVersion;
use crate::values::pack_stream_value::PackStreamStruct;

pub(crate) const TAG_DATE: u8 = 0x44;
pub(crate) const TAG_DATE_TIME_V1: u8 = 0x46;
pub(crate) const TAG_DATE_TIME_V2: u8 = 0x49;
pub(crate) const TAG_DATE_TIME_ZONE_ID_V1: u8 = 0x66;
pub(crate) const TAG_DATE_TIME_ZONE_ID_V2: u8 = 0x69;
pub(crate) const TAG_DURATION: u8 = 0x45;
pub(crate) const TAG_LOCAL_DATE_TIME: u8 = 0x64;
pub(crate) const TAG_LOCAL_TIME: u8 = 0x74;
pub(crate) const TAG_NODE: u8 = 0x4E;
pub(crate) const TAG_PATH: u8 = 0x50;
pub(crate) const TAG_POINT_2D: u8 = 0x58;
pub(crate) const TAG_POINT_3D: u8 = 0x59;
pub(crate) const TAG_RELATIONSHIP: u8 = 0x52;
pub(crate) const TAG_TIME: u8 = 0x54;
pub(crate) const TAG_UNBOUND_RELATIONSHIP: u8 = 0x72;
pub(crate) const TAG_VECTOR: u8 = 0x56;
pub(crate) const TAG_UNSUPPORTED_TYPE: u8 = 0x3F;

#[derive(Debug)]
pub(crate) struct BoltStruct<'a> {
    inner: BoltStructType<'a>,
}

impl<'a> From<BoltStructType<'a>> for BoltStruct<'a> {
    fn from(inner: BoltStructType<'a>) -> Self {
        Self { inner }
    }
}

#[derive(Debug)]
enum BoltStructType<'a> {
    Node(JoltNode<'a>),
    Relationship(JoltRelationship<'a>),
    Path(JoltPath<'a>),
    Point(JoltPoint),
    Date(JoltDate),
    Time(JoltTime<'a>),
    DateTime(JoltDateTime<'a>),
    Duration(JoltDuration),
    Vector(JoltVector),
    UnsupportedType(JoltUnsupportedType),
}

impl<'a> BoltStruct<'a> {
    pub(crate) fn read(value: &'a PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        BoltStructType::read(value, jolt_version).map(Into::into)
    }

    pub(crate) fn read_other_jolt_version(
        value: &'a PackStreamStruct,
        jolt_version: JoltVersion,
    ) -> Option<(Self, JoltVersion)> {
        for jolt_version in jolt_version.proximity_iter() {
            if let Some(struct_type) = BoltStructType::read(value, jolt_version) {
                return Some((struct_type.into(), jolt_version));
            }
        }
        None
    }

    pub(crate) fn jolt_serialize<S>(
        &self,
        serializer: S,
        jolt_version: JoltVersion,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inner.jolt_fmt(serializer, jolt_version)
    }
}

impl<'a> BoltStructType<'a> {
    pub(crate) fn read(value: &'a PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        JoltNode::from_struct(value, jolt_version)
            .map(Self::Node)
            .or_else(|| JoltRelationship::from_struct(value, jolt_version).map(Self::Relationship))
            .or_else(|| JoltPath::from_struct(value, jolt_version).map(Self::Path))
            .or_else(|| JoltPoint::from_struct(value, jolt_version).map(Self::Point))
            .or_else(|| JoltDate::from_struct(value, jolt_version).map(Self::Date))
            .or_else(|| JoltTime::from_struct(value, jolt_version).map(Self::Time))
            .or_else(|| JoltDateTime::from_struct(value, jolt_version).map(Self::DateTime))
            .or_else(|| JoltDuration::from_struct(value, jolt_version).map(Self::Duration))
            .or_else(|| JoltVector::from_struct(value, jolt_version).map(Self::Vector))
            .or_else(|| {
                JoltUnsupportedType::from_struct(value, jolt_version).map(Self::UnsupportedType)
            })
    }

    pub(crate) fn jolt_fmt<S>(
        &self,
        serializer: S,
        jolt_version: JoltVersion,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let ser = serializer;
        let ver = jolt_version;

        match self {
            BoltStructType::Node(v) => v.jolt_serialize(ser, ver),
            BoltStructType::Relationship(v) => v.jolt_serialize(ser, ver),
            BoltStructType::Path(v) => v.jolt_serialize(ser, ver),
            BoltStructType::Point(v) => v.jolt_serialize(ser, ver),
            BoltStructType::Date(v) => v.jolt_serialize(ser, ver),
            BoltStructType::Time(v) => v.jolt_serialize(ser, ver),
            BoltStructType::DateTime(v) => v.jolt_serialize(ser, ver),
            BoltStructType::Duration(v) => v.jolt_serialize(ser, ver),
            BoltStructType::Vector(v) => v.jolt_serialize(ser, ver),
            BoltStructType::UnsupportedType(v) => v.jolt_serialize(ser, ver),
        }
    }
}

#[cfg(test)]
mod tests {}
