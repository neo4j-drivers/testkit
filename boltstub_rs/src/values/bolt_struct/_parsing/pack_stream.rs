use indexmap::IndexMap;

use crate::values::pack_stream_value::PackStreamValue;

pub(in super::super) fn next_pack_stream_field<'a, R: ExtractableField<'a>>(
    mut fields: impl Iterator<Item = &'a PackStreamValue>,
) -> Option<R> {
    R::extract(fields.next()?)
}

pub(in super::super) fn check_last_pack_stream_field<'a>(
    mut fields: impl Iterator<Item = &'a PackStreamValue>,
) -> bool {
    fields.next().is_none()
}

pub(in super::super) trait ExtractableField<'a>:
    private::Sealed + Sized + 'a
{
    fn extract(field: &'a PackStreamValue) -> Option<Self>;
}

mod private {
    pub trait Sealed {}
}

impl private::Sealed for i64 {}
impl<'a> ExtractableField<'a> for i64 {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field.as_int()
    }
}

impl private::Sealed for Vec<i64> {}
impl<'a> ExtractableField<'a> for Vec<i64> {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field
            .as_list()?
            .iter()
            .map(PackStreamValue::as_int)
            .collect()
    }
}

impl private::Sealed for &[u8] {}
impl<'a> ExtractableField<'a> for &'a [u8] {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field.as_bytes().map(Vec::as_slice)
    }
}

impl private::Sealed for i32 {}
impl<'a> ExtractableField<'a> for i32 {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field.as_int().and_then(|i| i.try_into().ok())
    }
}

impl private::Sealed for f64 {}
impl<'a> ExtractableField<'a> for f64 {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field.as_float()
    }
}

impl private::Sealed for Vec<&String> {}
impl<'a> ExtractableField<'a> for Vec<&'a String> {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field.as_list()?.iter().map(|l| l.as_string()).collect()
    }
}

impl private::Sealed for Vec<&str> {}
impl<'a> ExtractableField<'a> for Vec<&'a str> {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field
            .as_list()?
            .iter()
            .map(|l| l.as_string().map(String::as_str))
            .collect()
    }
}

impl private::Sealed for &[PackStreamValue] {}
impl<'a> ExtractableField<'a> for &'a [PackStreamValue] {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field.as_list()
    }
}

impl private::Sealed for &IndexMap<String, PackStreamValue> {}
impl<'a> ExtractableField<'a> for &'a IndexMap<String, PackStreamValue> {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field.as_map()
    }
}

impl private::Sealed for &String {}
impl<'a> ExtractableField<'a> for &'a String {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field.as_string()
    }
}

impl private::Sealed for &str {}
impl<'a> ExtractableField<'a> for &'a str {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field.as_string().map(String::as_str)
    }
}
