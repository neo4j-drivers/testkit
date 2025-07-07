use std::io::{Result as IoResult, Write};

use serde::Serialize;
use serde_json::ser::Formatter as JsonFormatter;
use serde_json::Result as JsonResult;

struct PrettyCompactFormatter {}

impl JsonFormatter for PrettyCompactFormatter {
    #[inline]
    fn begin_array_value<W>(&mut self, writer: &mut W, first: bool) -> IoResult<()>
    where
        W: ?Sized + Write,
    {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    #[inline]
    fn begin_object_key<W>(&mut self, writer: &mut W, first: bool) -> IoResult<()>
    where
        W: ?Sized + Write,
    {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }
}

pub fn compact_pretty_print<T>(value: &T) -> JsonResult<String>
where
    T: ?Sized + Serialize,
{
    let mut writer = Vec::with_capacity(128);
    let mut ser = serde_json::Serializer::with_formatter(&mut writer, PrettyCompactFormatter {});
    value.serialize(&mut ser)?;
    Ok(String::from_utf8(writer).expect("serde_json always produces valid UTF-8"))
}
