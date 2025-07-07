use indexmap::IndexMap;

use crate::values::pack_stream_value::PackStreamValue;

#[allow(private_bounds)]
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

trait ExtractableField<'a>: Sized + 'a {
    fn extract(field: &'a PackStreamValue) -> Option<Self>;
}

impl<'a> ExtractableField<'a> for i64 {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field.as_int()
    }
}

impl<'a> ExtractableField<'a> for Vec<i64> {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field.as_list()?.iter().map(|l| l.as_int()).collect()
    }
}

impl<'a> ExtractableField<'a> for i32 {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field.as_int().and_then(|i| i.try_into().ok())
    }
}

impl<'a> ExtractableField<'a> for f64 {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field.as_float()
    }
}

impl<'a> ExtractableField<'a> for Vec<&'a String> {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field.as_list()?.iter().map(|l| l.as_string()).collect()
    }
}

impl<'a> ExtractableField<'a> for Vec<&'a str> {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field
            .as_list()?
            .iter()
            .map(|l| l.as_string().map(String::as_str))
            .collect()
    }
}

impl<'a> ExtractableField<'a> for &'a [PackStreamValue] {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field.as_list()
    }
}

impl<'a> ExtractableField<'a> for &'a IndexMap<String, PackStreamValue> {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field.as_map()
    }
}

impl<'a> ExtractableField<'a> for &'a String {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field.as_string()
    }
}

impl<'a> ExtractableField<'a> for &'a str {
    fn extract(field: &'a PackStreamValue) -> Option<Self> {
        field.as_string().map(String::as_str)
    }
}
