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
use std::fmt::{Display, Formatter};
use std::io::Write;

use anyhow::{anyhow, Result};
use indexmap::IndexMap;
use nom::ToUsize;
use usize_cast::FromUsize;

use crate::bolt_version::JoltVersion;
use crate::jolt::JoltSigil;
use crate::str_bytes;
use crate::values::bolt_struct::BoltStruct;

#[allow(unused)]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PackStreamValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<PackStreamValue>),
    Dict(IndexMap<String, PackStreamValue>),
    Struct(PackStreamStruct),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackStreamStruct {
    pub tag: u8,
    pub fields: Vec<PackStreamValue>,
}

impl PackStreamValue {
    pub(crate) fn from_data_consume_all(data: &[u8]) -> Result<PackStreamValue> {
        let mut decoder = PackStreamDecoder::new(data, 0);
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

    pub(crate) fn as_data(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(128);
        let mut serializer = PackStreamSerializer::new(&mut data);
        serializer.write(self);
        data
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
                    v1.is_nan() && v2.is_nan() || v1.to_bits() == v2.to_bits()
                }
                _ => false,
            },
            PackStreamValue::Bytes(v1) => matches!(other, PackStreamValue::Bytes(v2) if v1 == v2),
            PackStreamValue::String(v1) => matches!(other, PackStreamValue::String(v2) if v1 == v2),
            PackStreamValue::List(v1) => matches!(other, PackStreamValue::List(v2) if v1 == v2),
            PackStreamValue::Dict(v1) => matches!(other, PackStreamValue::Dict(v2) if v1 == v2),
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
impl_value_from_owned!(PackStreamValue::Struct, PackStreamStruct);
// impl_value_from_owned!(Value::List, Vec<Value>);
// impl_value_from_owned!(Value::Map, HashMap<String, Value>);
impl<T: Into<PackStreamValue>> From<IndexMap<String, T>> for PackStreamValue {
    fn from(value: IndexMap<String, T>) -> Self {
        PackStreamValue::Dict(value.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

impl<T: Into<PackStreamValue>> From<Vec<T>> for PackStreamValue {
    fn from(value: Vec<T>) -> Self {
        PackStreamValue::List(value.into_iter().map(|v| v.into()).collect())
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
    #[allow(clippy::result_large_err)]
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
    #[allow(clippy::result_large_err)]
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
    #[allow(clippy::result_large_err)]
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
    #[allow(clippy::result_large_err)]
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
    #[allow(clippy::result_large_err)]
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
    #[allow(clippy::result_large_err)]
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
    #[allow(clippy::result_large_err)]
    pub fn try_into_map(self) -> Result<IndexMap<String, PackStreamValue>, Self> {
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
    #[allow(clippy::result_large_err)]
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

struct PackStreamDecoder<'a> {
    bytes: &'a [u8],
    index: usize,
}

impl<'a> PackStreamDecoder<'a> {
    fn new(bytes: &'a [u8], idx: usize) -> Self {
        Self { bytes, index: idx }
    }

    fn read(&mut self) -> Result<PackStreamValue> {
        let marker = self.read_byte()?;
        self.read_value(marker)
    }

    fn read_value(&mut self, marker: u8) -> Result<PackStreamValue> {
        let high_nibble = marker & 0xF0;

        Ok(match marker {
            // tiny int
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
                self.read_bytes(len)?
            }
            BYTES_16 => {
                let len = self.read_u16()?;
                self.read_bytes(len)?
            }
            BYTES_32 => {
                let len = self.read_u32()?;
                self.read_bytes(len)?
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
            _ if high_nibble == TINY_STRUCT => self.read_struct((marker & 0x0F).into())?,
            _ => {
                // raise ValueError("Unknown PackStream marker %02X" % marker)
                return Err(anyhow!("Unknown PackStream marker {:02X}", marker));
            }
        })
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

    fn read_bytes(&mut self, length: usize) -> Result<PackStreamValue> {
        let data = self.bytes.get(self.index..self.index + length);
        self.index += length;
        Ok(data.into())
    }

    fn read_struct(&mut self, length: usize) -> Result<PackStreamValue> {
        let tag = self.read_byte()?;
        let mut fields = Vec::with_capacity(length);
        for _ in 0..length {
            fields.push(self.read()?)
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
            _ => Err(anyhow!("Invalid string length marker: {}", marker)),
        }
    }

    fn read_byte(&mut self) -> Result<u8> {
        let byte = *self
            .bytes
            .get(self.index)
            .ok_or_else(|| anyhow!("Nothing to unpack"))?;
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
            None => Err(anyhow!("no me gusta")),
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

    fn read_raw_string(&mut self, length: usize) -> Result<String> {
        if length == 0 {
            return Ok("".into());
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
    data: &'a mut Vec<u8>,
}

impl<'a> PackStreamSerializer<'a> {
    pub fn new(writer: &'a mut Vec<u8>) -> Self {
        Self { data: writer }
    }

    pub fn write(&mut self, value: &PackStreamValue) {
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
                    self.write(value);
                }
            }
            PackStreamValue::Dict(m) => {
                self.write_dict_header(u64::from_usize(m.len()));
                for (k, v) in m {
                    self.write_string(k);
                    self.write(v);
                }
            }
            PackStreamValue::Struct(PackStreamStruct { tag, fields }) => {
                self.write_struct_header(
                    *tag,
                    fields
                        .len()
                        .try_into()
                        .expect("Produced struct with too many fields"),
                );
                for value in fields {
                    self.write(value);
                }
            }
        }
    }

    #[inline]
    fn write_all(&mut self, data: &[u8]) {
        self.data.write_all(data).unwrap();
    }

    fn write_null(&mut self) {
        self.write_all(&[0xC0]);
    }

    fn write_bool(&mut self, b: bool) {
        self.write_all(match b {
            false => &[0xC2],
            true => &[0xC3],
        });
    }

    fn write_int(&mut self, i: i64) {
        if (-16..=127).contains(&i) {
            self.write_all(&i8::to_be_bytes(i as i8));
        } else if (-128..=127).contains(&i) {
            self.write_all(&[0xC8]);
            self.write_all(&i8::to_be_bytes(i as i8));
        } else if (-32_768..=32_767).contains(&i) {
            self.write_all(&[0xC9]);
            self.write_all(&i16::to_be_bytes(i as i16));
        } else if (-2_147_483_648..=2_147_483_647).contains(&i) {
            self.write_all(&[0xCA]);
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
            self.write_all(&u8::to_be_bytes(size as u8));
        } else if size <= 65_535 {
            self.write_all(&[0xCD]);
            self.write_all(&u16::to_be_bytes(size as u16));
        } else if size <= 2_147_483_647 {
            self.write_all(&[0xCE]);
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
            self.write_all(&[0x80 + size as u8]);
        } else if size <= 255 {
            self.write_all(&[0xD0]);
            self.write_all(&u8::to_be_bytes(size as u8));
        } else if size <= 65_535 {
            self.write_all(&[0xD1]);
            self.write_all(&u16::to_be_bytes(size as u16));
        } else if size <= 2_147_483_647 {
            self.write_all(&[0xD2]);
            self.write_all(&u32::to_be_bytes(size as u32));
        } else {
            panic!("string exceeds max size of 2,147,483,647 bytes");
        }
        self.write_all(bytes);
    }

    fn write_list_header(&mut self, size: u64) {
        if size <= 15 {
            self.write_all(&[0x90 + size as u8]);
        } else if size <= 255 {
            self.write_all(&[0xD4]);
            self.write_all(&u8::to_be_bytes(size as u8));
        } else if size <= 65_535 {
            self.write_all(&[0xD5]);
            self.write_all(&u16::to_be_bytes(size as u16));
        } else if size <= 2_147_483_647 {
            self.write_all(&[0xD6]);
            self.write_all(&u32::to_be_bytes(size as u32));
        } else {
            panic!("list exceeds max size of 2,147,483,647");
        }
    }

    fn write_dict_header(&mut self, size: u64) {
        if size <= 15 {
            self.write_all(&[0xA0 + size as u8]);
        } else if size <= 255 {
            self.write_all(&[0xD8]);
            self.write_all(&u8::to_be_bytes(size as u8));
        } else if size <= 65_535 {
            self.write_all(&[0xD9]);
            self.write_all(&u16::to_be_bytes(size as u16));
        } else if size <= 2_147_483_647 {
            self.write_all(&[0xDA]);
            self.write_all(&u32::to_be_bytes(size as u32));
        } else {
            panic!("map exceeds max size of 2,147,483,647");
        }
    }

    fn write_struct_header(&mut self, tag: u8, size: u8) {
        self.write_all(&[0xB0 + size, tag]);
    }
}

pub(crate) fn value_jolt_fmt(
    value: &PackStreamValue,
    jolt_version: JoltVersion,
) -> impl Display + '_ {
    struct Repr<'a> {
        value: &'a PackStreamValue,
        jolt_version: JoltVersion,
    }

    impl Display for Repr<'_> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self.value {
                PackStreamValue::Null => f.write_str("null"),
                PackStreamValue::Boolean(b) => Display::fmt(&b, f),
                PackStreamValue::Integer(i) => Display::fmt(&i, f),
                PackStreamValue::Float(v) => Display::fmt(&v, f),
                PackStreamValue::Bytes(b) => {
                    f.write_str(r##"{{"#": "##)?;
                    str_bytes::fmt_bytes(b).fmt(f)?;
                    f.write_str("}")
                }
                PackStreamValue::String(s) => std::fmt::Debug::fmt(&s, f),
                PackStreamValue::List(l) => {
                    f.write_str("[")?;
                    write_joined_values(f, l, self.jolt_version)?;
                    f.write_str("]")
                }
                PackStreamValue::Dict(m) => {
                    let (start, end) = match m.keys().next().and_then(|k| JoltSigil::from_str(k)) {
                        Some(_) if m.len() == 1 => {
                            // dict could be confused with Jolt => encode as Jolt Dict
                            (r#"{"{}": {"#, "}}")
                        }
                        _ => ("{", "}"),
                    };
                    f.write_str(start)?;
                    write_joined_entries(f, m.iter(), self.jolt_version)?;
                    f.write_str(end)
                }
                PackStreamValue::Struct(s) => match BoltStruct::read(s, self.jolt_version) {
                    None => {
                        write!(f, "Struct[{:#04X}]{{", s.tag)?;
                        write_joined_values(f, &s.fields, self.jolt_version)?;
                        f.write_str("}}")
                    }
                    Some(bolt_struct) => Display::fmt(&bolt_struct.jolt_fmt(self.jolt_version), f),
                },
            }
        }
    }

    Repr {
        value,
        jolt_version,
    }
}

pub(super) fn write_joined_values(
    f: &mut Formatter<'_>,
    values: &[PackStreamValue],
    jolt_version: JoltVersion,
) -> std::fmt::Result {
    let mut values = values.iter();
    if let Some(value) = values.next() {
        Display::fmt(&value_jolt_fmt(value, jolt_version), f)?;
        for value in values {
            f.write_str(", ")?;
            Display::fmt(&value_jolt_fmt(value, jolt_version), f)?;
        }
    }
    Ok(())
}

pub(super) fn write_joined_entries<'e>(
    f: &mut Formatter<'_>,
    mut entries: impl Iterator<Item = (&'e String, &'e PackStreamValue)>,
    jolt_version: JoltVersion,
) -> std::fmt::Result {
    if let Some((k, v)) = entries.next() {
        std::fmt::Debug::fmt(k, f)?;
        f.write_str(": ")?;
        Display::fmt(&value_jolt_fmt(v, jolt_version), f)?;
        for (k, v) in entries {
            f.write_str(", ")?;
            std::fmt::Debug::fmt(k, f)?;
            f.write_str(": ")?;
            Display::fmt(&value_jolt_fmt(v, jolt_version), f)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::tiny_int_min(&[0xF0], PackStreamValue::Integer(-16))]
    #[case::tiny_int_m1(&[0xFF], PackStreamValue::Integer(-1))]
    #[case::tiny_int_0(&[0x00], PackStreamValue::Integer(0))]
    #[case::tiny_int_p1(&[0x01], PackStreamValue::Integer(1))]
    #[case::tiny_int_max(&[0x7F], PackStreamValue::Integer(127))]
    fn decode(#[case] bytes: &[u8], #[case] expected: PackStreamValue) {
        let mut decoder = PackStreamDecoder::new(bytes, 0);
        let value = decoder.read().unwrap();
        assert_eq!(value, expected);
    }
}
