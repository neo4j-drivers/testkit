// Copyright Rouven Bauer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::convert::Into;
use std::io::Write;

use anyhow::{anyhow, Result};
use indexmap::IndexMap;
use nom::ToUsize;
use usize_cast::FromUsize;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, derive_more::Display)]
pub enum PackStreamVersion {
    #[display("PackStream v1")]
    V1,
    #[display("PackStream v2")]
    V2,
}

#[derive(Debug, Clone)]
pub enum PackStreamValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<PackStreamValue>),
    Dict(IndexMap<String, PackStreamValue>),
    Uuid(Uuid),
    Struct(PackStreamStruct),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackStreamStruct {
    pub tag: u8,
    pub fields: Vec<PackStreamValue>,
}

impl PackStreamVersion {
    pub(crate) fn supports_uuid(self) -> bool {
        match self {
            PackStreamVersion::V1 => false,
            PackStreamVersion::V2 => true,
        }
    }
}

impl PackStreamValue {
    pub(crate) fn from_data_consume_all(
        data: &[u8],
        version: PackStreamVersion,
    ) -> Result<PackStreamValue> {
        let mut decoder = PackStreamDecoder::new(version, data, 0);
        let value = decoder.read()?;
        if decoder.index != data.len() {
            return Err(anyhow!(
                "Unconsumed data ({} bytes) {:?} read: {:?}",
                data.len() - decoder.index,
                data,
                value
            ));
        }
        Ok(value)
    }

    pub(crate) fn as_data(&self, version: PackStreamVersion) -> Result<Vec<u8>> {
        let mut data = Vec::with_capacity(128);
        let mut serializer = PackStreamSerializer::new(version, &mut data);
        serializer.write(self)?;
        Ok(data)
    }
}

impl PartialEq for PackStreamValue {
    fn eq(&self, other: &Self) -> bool {
        match self {
            PackStreamValue::Null => matches!(other, PackStreamValue::Null),
            PackStreamValue::Boolean(v1) => {
                matches!(other, PackStreamValue::Boolean(v2) if v1 == v2)
            }
            PackStreamValue::Integer(v1) => {
                matches!(other, PackStreamValue::Integer(v2) if v1 == v2)
            }
            PackStreamValue::Float(v1) => match other {
                PackStreamValue::Float(v2) => {
                    v1.to_bits() == v2.to_bits() || v1.is_nan() && v2.is_nan()
                }
                _ => false,
            },
            PackStreamValue::Bytes(v1) => matches!(other, PackStreamValue::Bytes(v2) if v1 == v2),
            PackStreamValue::String(v1) => matches!(other, PackStreamValue::String(v2) if v1 == v2),
            PackStreamValue::List(v1) => matches!(other, PackStreamValue::List(v2) if v1 == v2),
            PackStreamValue::Dict(v1) => matches!(other, PackStreamValue::Dict(v2) if v1 == v2),
            PackStreamValue::Uuid(v1) => matches!(other, PackStreamValue::Uuid(v2) if v1 == v2),
            PackStreamValue::Struct(v1) => matches!(other, PackStreamValue::Struct(v2) if v1 == v2),
        }
    }
}

impl Eq for PackStreamValue {}

macro_rules! impl_value_from_into {
    ( $value:expr, $($ty:ty),* ) => {
        $(
            impl From<$ty> for PackStreamValue {
                fn from(value: $ty) -> Self {
                    $value(value.into())
                }
            }
        )*
    };
}

macro_rules! impl_value_from_owned {
    ( $value:expr, $($ty:ty),* ) => {
        $(
            impl From<$ty> for PackStreamValue {
                fn from(value: $ty) -> Self {
                    $value(value)
                }
            }
        )*
    };
}

impl_value_from_into!(PackStreamValue::Boolean, bool);
impl_value_from_into!(PackStreamValue::Integer, u8, u16, u32, i8, i16, i32, i64);
impl_value_from_into!(PackStreamValue::Float, f32, f64);
impl_value_from_into!(PackStreamValue::String, &str);
impl_value_from_into!(PackStreamValue::Bytes, &[u8]);

impl_value_from_owned!(PackStreamValue::String, String);
impl_value_from_owned!(PackStreamValue::Uuid, Uuid);
impl_value_from_owned!(PackStreamValue::Struct, PackStreamStruct);

impl<T: Into<PackStreamValue>> From<IndexMap<String, T>> for PackStreamValue {
    fn from(value: IndexMap<String, T>) -> Self {
        PackStreamValue::Dict(value.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

impl<T: Into<PackStreamValue>> From<Vec<T>> for PackStreamValue {
    fn from(value: Vec<T>) -> Self {
        PackStreamValue::List(value.into_iter().map(std::convert::Into::into).collect())
    }
}

impl<T: Into<PackStreamValue>> From<Option<T>> for PackStreamValue {
    fn from(value: Option<T>) -> Self {
        match value {
            None => PackStreamValue::Null,
            Some(v) => v.into(),
        }
    }
}

impl PackStreamValue {
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, PackStreamValue::Null)
    }
}

impl TryFrom<PackStreamValue> for bool {
    type Error = PackStreamValue;

    #[inline]
    fn try_from(value: PackStreamValue) -> Result<Self, Self::Error> {
        match value {
            PackStreamValue::Boolean(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl PackStreamValue {
    #[inline]
    pub fn is_bool(&self) -> bool {
        matches!(self, PackStreamValue::Boolean(_))
    }

    #[inline]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            PackStreamValue::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err, reason = "DX over performance")]
    pub fn try_into_bool(self) -> Result<bool, Self> {
        self.try_into()
    }
}

impl TryFrom<PackStreamValue> for i64 {
    type Error = PackStreamValue;

    #[inline]
    fn try_from(value: PackStreamValue) -> Result<Self, Self::Error> {
        match value {
            PackStreamValue::Integer(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl PackStreamValue {
    #[inline]
    pub fn is_int(&self) -> bool {
        matches!(self, PackStreamValue::Integer(_))
    }

    #[inline]
    pub fn as_int(&self) -> Option<i64> {
        match self {
            PackStreamValue::Integer(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err, reason = "DX over performance")]
    pub fn try_into_int(self) -> Result<i64, Self> {
        self.try_into()
    }
}

impl TryFrom<PackStreamValue> for f64 {
    type Error = PackStreamValue;

    #[inline]
    fn try_from(value: PackStreamValue) -> Result<Self, Self::Error> {
        match value {
            PackStreamValue::Float(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl PackStreamValue {
    #[inline]
    pub fn is_float(&self) -> bool {
        matches!(self, PackStreamValue::Float(_))
    }

    #[inline]
    pub fn as_float(&self) -> Option<f64> {
        match self {
            PackStreamValue::Float(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err, reason = "DX over performance")]
    pub fn try_into_float(self) -> Result<f64, Self> {
        self.try_into()
    }
}

impl TryFrom<PackStreamValue> for Vec<u8> {
    type Error = PackStreamValue;

    #[inline]
    fn try_from(value: PackStreamValue) -> Result<Self, Self::Error> {
        match value {
            PackStreamValue::Bytes(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl PackStreamValue {
    #[inline]
    pub fn is_bytes(&self) -> bool {
        matches!(self, PackStreamValue::Bytes(_))
    }

    #[inline]
    pub fn as_bytes(&self) -> Option<&Vec<u8>> {
        match self {
            PackStreamValue::Bytes(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err, reason = "DX over performance")]
    pub fn try_into_bytes(self) -> Result<Vec<u8>, Self> {
        self.try_into()
    }
}

impl TryFrom<PackStreamValue> for String {
    type Error = PackStreamValue;

    #[inline]
    fn try_from(value: PackStreamValue) -> Result<Self, Self::Error> {
        match value {
            PackStreamValue::String(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl PackStreamValue {
    #[inline]
    pub fn is_string(&self) -> bool {
        matches!(self, PackStreamValue::String(_))
    }

    #[inline]
    pub fn as_string(&self) -> Option<&String> {
        match self {
            PackStreamValue::String(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err, reason = "DX over performance")]
    pub fn try_into_string(self) -> Result<String, Self> {
        self.try_into()
    }
}

impl TryFrom<PackStreamValue> for Vec<PackStreamValue> {
    type Error = PackStreamValue;

    #[inline]
    fn try_from(value: PackStreamValue) -> Result<Self, Self::Error> {
        match value {
            PackStreamValue::List(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl PackStreamValue {
    #[inline]
    pub fn is_list(&self) -> bool {
        matches!(self, PackStreamValue::List(_))
    }

    #[inline]
    pub fn as_list(&self) -> Option<&[PackStreamValue]> {
        match self {
            PackStreamValue::List(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err, reason = "DX over performance")]
    pub fn try_into_list(self) -> Result<Vec<PackStreamValue>, Self> {
        self.try_into()
    }
}

impl TryFrom<PackStreamValue> for IndexMap<String, PackStreamValue> {
    type Error = PackStreamValue;

    #[inline]
    fn try_from(value: PackStreamValue) -> Result<Self, Self::Error> {
        match value {
            PackStreamValue::Dict(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl PackStreamValue {
    #[inline]
    pub fn is_map(&self) -> bool {
        matches!(self, PackStreamValue::Dict(_))
    }

    #[inline]
    pub fn as_map(&self) -> Option<&IndexMap<String, PackStreamValue>> {
        match self {
            PackStreamValue::Dict(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err, reason = "DX over performance")]
    pub fn try_into_map(self) -> Result<IndexMap<String, PackStreamValue>, Self> {
        self.try_into()
    }
}

impl TryFrom<PackStreamValue> for Uuid {
    type Error = PackStreamValue;

    #[inline]
    fn try_from(value: PackStreamValue) -> Result<Self, Self::Error> {
        match value {
            PackStreamValue::Uuid(uuid) => Ok(uuid),
            _ => Err(value),
        }
    }
}

impl PackStreamValue {
    #[inline]
    pub fn is_uuid(&self) -> bool {
        matches!(self, PackStreamValue::Uuid(_))
    }

    #[inline]
    pub fn as_uuid(&self) -> Option<Uuid> {
        match self {
            PackStreamValue::Uuid(uuid) => Some(*uuid),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err, reason = "DX over performance")]
    pub fn try_into_uuid(self) -> Result<Uuid, Self> {
        self.try_into()
    }
}

impl TryFrom<PackStreamValue> for PackStreamStruct {
    type Error = PackStreamValue;

    #[inline]
    fn try_from(value: PackStreamValue) -> Result<Self, Self::Error> {
        match value {
            PackStreamValue::Struct(struct_) => Ok(struct_),
            _ => Err(value),
        }
    }
}

impl PackStreamValue {
    #[inline]
    pub fn is_struct(&self) -> bool {
        matches!(self, PackStreamValue::Struct(_))
    }

    #[inline]
    pub fn as_struct(&self) -> Option<&PackStreamStruct> {
        match self {
            PackStreamValue::Struct(struct_) => Some(struct_),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err, reason = "DX over performance")]
    pub fn try_into_struct(self) -> Result<PackStreamStruct, Self> {
        self.try_into()
    }
}

const TINY_STRING: u8 = 0x80;
const TINY_LIST: u8 = 0x90;
const TINY_MAP: u8 = 0xA0;
const TINY_STRUCT: u8 = 0xB0;
const NULL: u8 = 0xC0;
const FALSE: u8 = 0xC2;
const TRUE: u8 = 0xC3;
const INT_8: u8 = 0xC8;
const INT_16: u8 = 0xC9;
const INT_32: u8 = 0xCA;
const INT_64: u8 = 0xCB;
const FLOAT_64: u8 = 0xC1;
const STRING_8: u8 = 0xD0;
const STRING_16: u8 = 0xD1;
const STRING_32: u8 = 0xD2;
const LIST_8: u8 = 0xD4;
const LIST_16: u8 = 0xD5;
const LIST_32: u8 = 0xD6;
const MAP_8: u8 = 0xD8;
const MAP_16: u8 = 0xD9;
const MAP_32: u8 = 0xDA;
const BYTES_8: u8 = 0xCC;
const BYTES_16: u8 = 0xCD;
const BYTES_32: u8 = 0xCE;
const UUID: u8 = 0xE0;

struct PackStreamDecoder<'a> {
    version: PackStreamVersion,
    bytes: &'a [u8],
    index: usize,
}

impl<'a> PackStreamDecoder<'a> {
    fn new(version: PackStreamVersion, bytes: &'a [u8], idx: usize) -> Self {
        Self {
            version,
            bytes,
            index: idx,
        }
    }

    fn read(&mut self) -> Result<PackStreamValue> {
        let marker = self.read_byte()?;
        self.read_value(marker)
    }

    fn read_value(&mut self, marker: u8) -> Result<PackStreamValue> {
        let high_nibble = marker & 0xF0;

        let value = match marker {
            // tiny int
            #[allow(clippy::cast_possible_wrap, reason = "deliberate wrapping")]
            _ if marker as i8 >= -16 => (marker as i8).into(),
            NULL => PackStreamValue::Null,
            FLOAT_64 => self.read_f64()?.into(),
            FALSE => false.into(),
            TRUE => true.into(),
            INT_8 => self.read_i8()?.into(),
            INT_16 => self.read_i16()?.into(),
            INT_32 => self.read_i32()?.into(),
            INT_64 => self.read_i64()?.into(),
            BYTES_8 => {
                let len = self.read_u8()?;
                self.read_bytes(len)
            }
            BYTES_16 => {
                let len = self.read_u16()?;
                self.read_bytes(len)
            }
            BYTES_32 => {
                let len = self.read_u32()?;
                self.read_bytes(len)
            }
            _ if high_nibble == TINY_STRING => self.read_string((marker & 0x0F).into())?,
            STRING_8 => {
                let len = self.read_u8()?;
                self.read_string(len)?
            }
            STRING_16 => {
                let len = self.read_u16()?;
                self.read_string(len)?
            }
            STRING_32 => {
                let len = self.read_u32()?;
                self.read_string(len)?
            }
            _ if high_nibble == TINY_LIST => self.read_list((marker & 0x0F).into())?,
            LIST_8 => {
                let len = self.read_u8()?;
                self.read_list(len)?
            }
            LIST_16 => {
                let len = self.read_u16()?;
                self.read_list(len)?
            }
            LIST_32 => {
                let len = self.read_u32()?;
                self.read_list(len)?
            }
            _ if high_nibble == TINY_MAP => self.read_map((marker & 0x0F).into())?,
            MAP_8 => {
                let len = self.read_u8()?;
                self.read_map(len)?
            }
            MAP_16 => {
                let len = self.read_u16()?;
                self.read_map(len)?
            }
            MAP_32 => {
                let len = self.read_u32()?;
                self.read_map(len)?
            }
            UUID if self.version >= PackStreamVersion::V2 => self.read_uuid()?.into(),
            _ if high_nibble == TINY_STRUCT => self.read_struct((marker & 0x0F).into())?,
            _ => {
                return Err(anyhow!(
                    "Unknown PackStream marker {marker:02X} for {}",
                    self.version
                ));
            }
        };

        Ok(value)
    }

    fn read_list(&mut self, length: usize) -> Result<PackStreamValue> {
        let mut items = Vec::with_capacity(length);
        for _ in 0..length {
            items.push(self.read()?);
        }
        Ok(items.into())
    }

    fn read_string(&mut self, length: usize) -> Result<PackStreamValue> {
        self.read_raw_string(length).map(Into::into)
    }

    fn read_map(&mut self, length: usize) -> Result<PackStreamValue> {
        let mut key_value_pairs = IndexMap::with_capacity(length);
        for _ in 0..length {
            let len = self.read_string_length()?;
            let key = self.read_raw_string(len)?;
            let value = self.read()?;
            key_value_pairs.insert(key, value);
        }
        Ok(key_value_pairs.into())
    }

    fn read_bytes(&mut self, length: usize) -> PackStreamValue {
        let data = self.bytes.get(self.index..self.index + length);
        self.index += length;
        data.into()
    }

    fn read_struct(&mut self, length: usize) -> Result<PackStreamValue> {
        let tag = self.read_byte()?;
        let mut fields = Vec::with_capacity(length);
        for _ in 0..length {
            fields.push(self.read()?);
        }
        let bolt_struct = PackStreamValue::Struct(PackStreamStruct { tag, fields });
        Ok(bolt_struct)
    }

    fn read_string_length(&mut self) -> Result<usize> {
        let marker = self.read_byte()?;
        let high_nibble = marker & 0xF0;
        match high_nibble {
            TINY_STRING => Ok((marker & 0x0F) as usize),
            STRING_8 => self.read_u8(),
            STRING_16 => self.read_u16(),
            STRING_32 => self.read_u32(),
            _ => Err(anyhow!("Invalid string length marker: {marker}")),
        }
    }

    fn read_byte(&mut self) -> Result<u8> {
        let byte = *self.bytes.get(self.index).ok_or_else(|| {
            anyhow!(
                "Unexpected end of stream at {} (index: {}, reading: 1)",
                self.bytes.len(),
                self.index,
            )
        })?;
        self.index += 1;
        Ok(byte)
    }

    fn read_n_bytes<const N: usize>(&mut self) -> Result<[u8; N]> {
        let to = self.index + N;
        match self.bytes.get(self.index..to) {
            Some(b) => {
                self.index = to;
                Ok(<[u8; N]>::try_from(b).expect("we know the slice has exactly N values"))
            }
            None => Err(anyhow!(
                "Unexpected end of stream at {} (index: {}, reading: {N})",
                self.bytes.len(),
                self.index,
            )),
        }
    }

    fn read_u8(&mut self) -> Result<usize> {
        self.read_byte().map(Into::into)
    }

    fn read_u16(&mut self) -> Result<usize> {
        let data = self.read_n_bytes()?;
        Ok(u16::from_be_bytes(data).to_usize())
    }

    fn read_u32(&mut self) -> Result<usize> {
        let data = self.read_n_bytes()?;
        Ok(u32::from_be_bytes(data).to_usize())
    }

    fn read_i8(&mut self) -> Result<i8> {
        self.read_byte().map(|b| i8::from_be_bytes([b]))
    }

    fn read_i16(&mut self) -> Result<i16> {
        self.read_n_bytes().map(i16::from_be_bytes)
    }

    fn read_i32(&mut self) -> Result<i32> {
        self.read_n_bytes().map(i32::from_be_bytes)
    }

    fn read_i64(&mut self) -> Result<i64> {
        self.read_n_bytes().map(i64::from_be_bytes)
    }

    fn read_f64(&mut self) -> Result<f64> {
        self.read_n_bytes().map(f64::from_be_bytes)
    }

    fn read_uuid(&mut self) -> Result<Uuid> {
        self.read_n_bytes().map(Uuid::from_bytes)
    }

    fn read_raw_string(&mut self, length: usize) -> Result<String> {
        if length == 0 {
            return Ok(String::new());
        }
        let data = self
            .bytes
            .get(self.index..self.index + length)
            .ok_or(anyhow!("expected some bytes"))?;
        let parsed = String::from_utf8(data.into())?;
        self.index += length;
        Ok(parsed)
    }
}

pub struct PackStreamSerializer<'a> {
    version: PackStreamVersion,
    data: &'a mut Vec<u8>,
}

impl<'a> PackStreamSerializer<'a> {
    pub fn new(version: PackStreamVersion, data: &'a mut Vec<u8>) -> Self {
        Self { version, data }
    }

    fn write(&mut self, value: &PackStreamValue) -> Result<()> {
        match value {
            PackStreamValue::Null => self.write_null(),
            PackStreamValue::Boolean(b) => self.write_bool(*b),
            PackStreamValue::Integer(i) => self.write_int(*i),
            PackStreamValue::Float(f) => self.write_float(*f),
            PackStreamValue::Bytes(b) => self.write_bytes(b),
            PackStreamValue::String(s) => self.write_string(s),
            PackStreamValue::List(l) => {
                self.write_list_header(u64::from_usize(l.len()));
                for value in l {
                    self.write(value)?;
                }
            }
            PackStreamValue::Dict(m) => {
                self.write_dict_header(u64::from_usize(m.len()));
                for (k, v) in m {
                    self.write_string(k);
                    self.write(v)?;
                }
            }
            PackStreamValue::Uuid(u) => self.write_uuid(*u)?,
            PackStreamValue::Struct(PackStreamStruct { tag, fields }) => {
                self.write_struct_header(
                    *tag,
                    fields
                        .len()
                        .try_into()
                        .expect("Produced struct with too many fields"),
                );
                for value in fields {
                    self.write(value)?;
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn write_all(&mut self, data: &[u8]) {
        self.data.write_all(data).unwrap();
    }

    fn write_null(&mut self) {
        self.write_all(&[0xC0]);
    }

    fn write_bool(&mut self, b: bool) {
        self.write_all(if b { &[0xC3] } else { &[0xC2] });
    }

    fn write_int(&mut self, i: i64) {
        if (-16..=127).contains(&i) {
            #[allow(clippy::cast_possible_truncation, reason = "range checked")]
            self.write_all(&i8::to_be_bytes(i as i8));
        } else if (-128..=127).contains(&i) {
            self.write_all(&[0xC8]);
            #[allow(clippy::cast_possible_truncation, reason = "range checked")]
            self.write_all(&i8::to_be_bytes(i as i8));
        } else if (-32_768..=32_767).contains(&i) {
            self.write_all(&[0xC9]);
            #[allow(clippy::cast_possible_truncation, reason = "range checked")]
            self.write_all(&i16::to_be_bytes(i as i16));
        } else if (-2_147_483_648..=2_147_483_647).contains(&i) {
            self.write_all(&[0xCA]);
            #[allow(clippy::cast_possible_truncation, reason = "range checked")]
            self.write_all(&i32::to_be_bytes(i as i32));
        } else {
            self.write_all(&[0xCB]);
            self.write_all(&i64::to_be_bytes(i));
        }
    }

    fn write_float(&mut self, f: f64) {
        self.write_all(&[0xC1]);
        self.write_all(&f64::to_be_bytes(f));
    }

    fn write_bytes(&mut self, b: &[u8]) {
        let size = b.len();
        if size <= 255 {
            self.write_all(&[0xCC]);
            #[allow(clippy::cast_possible_truncation, reason = "range checked")]
            self.write_all(&u8::to_be_bytes(size as u8));
        } else if size <= 65_535 {
            self.write_all(&[0xCD]);
            #[allow(clippy::cast_possible_truncation, reason = "range checked")]
            self.write_all(&u16::to_be_bytes(size as u16));
        } else if size <= 2_147_483_647 {
            self.write_all(&[0xCE]);
            #[allow(clippy::cast_possible_truncation, reason = "range checked")]
            self.write_all(&u32::to_be_bytes(size as u32));
        } else {
            panic!("bytes exceed max size of 2,147,483,647");
        }
        self.write_all(b);
    }

    fn write_string(&mut self, s: &str) {
        let bytes = s.as_bytes();
        let size = bytes.len();
        if size <= 15 {
            #[allow(clippy::cast_possible_truncation, reason = "range checked")]
            self.write_all(&[0x80 + size as u8]);
        } else if size <= 255 {
            self.write_all(&[0xD0]);
            #[allow(clippy::cast_possible_truncation, reason = "range checked")]
            self.write_all(&u8::to_be_bytes(size as u8));
        } else if size <= 65_535 {
            self.write_all(&[0xD1]);
            #[allow(clippy::cast_possible_truncation, reason = "range checked")]
            self.write_all(&u16::to_be_bytes(size as u16));
        } else if size <= 2_147_483_647 {
            self.write_all(&[0xD2]);
            #[allow(clippy::cast_possible_truncation, reason = "range checked")]
            self.write_all(&u32::to_be_bytes(size as u32));
        } else {
            panic!("string exceeds max size of 2,147,483,647 bytes");
        }
        self.write_all(bytes);
    }

    fn write_list_header(&mut self, size: u64) {
        if size <= 15 {
            #[allow(clippy::cast_possible_truncation, reason = "range checked")]
            self.write_all(&[0x90 + size as u8]);
        } else if size <= 255 {
            self.write_all(&[0xD4]);
            #[allow(clippy::cast_possible_truncation, reason = "range checked")]
            self.write_all(&u8::to_be_bytes(size as u8));
        } else if size <= 65_535 {
            self.write_all(&[0xD5]);
            #[allow(clippy::cast_possible_truncation, reason = "range checked")]
            self.write_all(&u16::to_be_bytes(size as u16));
        } else if size <= 2_147_483_647 {
            self.write_all(&[0xD6]);
            #[allow(clippy::cast_possible_truncation, reason = "range checked")]
            self.write_all(&u32::to_be_bytes(size as u32));
        } else {
            panic!("list exceeds max size of 2,147,483,647");
        }
    }

    fn write_dict_header(&mut self, size: u64) {
        if size <= 15 {
            #[allow(clippy::cast_possible_truncation, reason = "range checked")]
            self.write_all(&[0xA0 + size as u8]);
        } else if size <= 255 {
            self.write_all(&[0xD8]);
            #[allow(clippy::cast_possible_truncation, reason = "range checked")]
            self.write_all(&u8::to_be_bytes(size as u8));
        } else if size <= 65_535 {
            self.write_all(&[0xD9]);
            #[allow(clippy::cast_possible_truncation, reason = "range checked")]
            self.write_all(&u16::to_be_bytes(size as u16));
        } else if size <= 2_147_483_647 {
            self.write_all(&[0xDA]);
            #[allow(clippy::cast_possible_truncation, reason = "range checked")]
            self.write_all(&u32::to_be_bytes(size as u32));
        } else {
            panic!("map exceeds max size of 2,147,483,647");
        }
    }

    fn write_uuid(&mut self, uuid: Uuid) -> Result<()> {
        if self.version < PackStreamVersion::V2 {
            return Err(anyhow!("UUID is not supported in {}", self.version));
        }
        self.write_all(&[0xE0]);
        self.write_all(uuid.as_bytes());
        Ok(())
    }

    fn write_struct_header(&mut self, tag: u8, size: u8) {
        self.write_all(&[0xB0 + size, tag]);
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::tiny_int_min(&[0xF0], PackStreamValue::Integer(-16))]
    #[case::tiny_int_m1(&[0xFF], PackStreamValue::Integer(-1))]
    #[case::tiny_int_0(&[0x00], PackStreamValue::Integer(0))]
    #[case::tiny_int_p1(&[0x01], PackStreamValue::Integer(1))]
    #[case::tiny_int_max(&[0x7F], PackStreamValue::Integer(127))]
    fn decode(
        #[case] bytes: &[u8],
        #[case] expected: PackStreamValue,
        #[values(PackStreamVersion::V1, PackStreamVersion::V2)] version: PackStreamVersion,
    ) {
        let mut decoder = PackStreamDecoder::new(version, bytes, 0);
        let value = decoder.read().unwrap();
        assert_eq!(value, expected);
    }
}
