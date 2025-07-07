mod _common;
mod _parsing;
mod date;
mod date_time;
mod duration;
mod node;
mod path;
mod point;
mod relationship;
mod time;

use std::fmt::{Debug, Display, Formatter};

use date::BoltDate;
pub(crate) use date::JoltDate;
use date_time::BoltDateTime;
pub(crate) use date_time::JoltDateTime;
use duration::BoltDuration;
pub(crate) use duration::JoltDuration;
use node::BoltNode;
pub(crate) use node::JoltNode;
use path::BoltPath;
pub(crate) use path::JoltPath;
use point::BoltPoint;
pub(crate) use point::JoltPoint;
use relationship::BoltRelationship;
pub(crate) use relationship::JoltRelationship;
use time::BoltTime;
pub(crate) use time::JoltTime;

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
    Node(BoltNode<'a>),
    Relationship(BoltRelationship<'a>),
    Path(BoltPath<'a>),
    Point(BoltPoint),
    Date(BoltDate),
    Time(BoltTime<'a>),
    DateTime(BoltDateTime<'a>),
    Duration(BoltDuration),
}

impl<'a> BoltStruct<'a> {
    pub(crate) fn read(value: &'a PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        BoltStructType::read(value, jolt_version).map(Into::into)
    }

    pub(crate) fn jolt_fmt(&self, jolt_version: JoltVersion) -> impl Display + '_ {
        self.inner.jolt_fmt(jolt_version)
    }
}

impl<'a> BoltStructType<'a> {
    pub(crate) fn read(value: &'a PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        BoltNode::from_struct(value, jolt_version)
            .map(Self::Node)
            .or_else(|| BoltRelationship::from_struct(value, jolt_version).map(Self::Relationship))
            .or_else(|| BoltPath::from_struct(value, jolt_version).map(Self::Path))
            .or_else(|| BoltPoint::from_struct(value, jolt_version).map(Self::Point))
            .or_else(|| BoltDate::from_struct(value, jolt_version).map(Self::Date))
            .or_else(|| BoltTime::from_struct(value, jolt_version).map(Self::Time))
            .or_else(|| BoltDateTime::from_struct(value, jolt_version).map(Self::DateTime))
            .or_else(|| BoltDuration::from_struct(value, jolt_version).map(Self::Duration))
    }

    pub(crate) fn jolt_fmt(&self, jolt_version: JoltVersion) -> impl Display + '_ {
        struct Repr<'a> {
            type_: &'a BoltStructType<'a>,
            jolt_version: JoltVersion,
        }

        impl Display for Repr<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                match self.type_ {
                    BoltStructType::Node(v) => v.jolt_fmt(self.jolt_version).fmt(f),
                    BoltStructType::Relationship(v) => v.jolt_fmt(self.jolt_version).fmt(f),
                    BoltStructType::Path(v) => v.jolt_fmt(self.jolt_version).fmt(f),
                    BoltStructType::Point(v) => v.jolt_fmt(self.jolt_version).fmt(f),
                    BoltStructType::Date(v) => v.jolt_fmt(self.jolt_version).fmt(f),
                    BoltStructType::Time(v) => v.jolt_fmt(self.jolt_version).fmt(f),
                    BoltStructType::DateTime(v) => v.jolt_fmt(self.jolt_version).fmt(f),
                    BoltStructType::Duration(v) => v.jolt_fmt(self.jolt_version).fmt(f),
                }
            }
        }

        Repr {
            type_: self,
            jolt_version,
        }
    }
}
