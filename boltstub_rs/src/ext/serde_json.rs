use std::io;

use serde::Serialize;
use serde_json::ser::Formatter as JsonFormatter;
use serde_json::Result as JsonResult;

struct PrettyCompactFormatter {}

impl JsonFormatter for PrettyCompactFormatter {
    #[inline]
    fn begin_array_value<W>(&mut self, writer: &mut W, first: bool) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    #[inline]
    fn begin_object_key<W>(&mut self, writer: &mut W, first: bool) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    #[inline]
    fn begin_object_value<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(b": ")
    }
}

pub fn to_writer_pretty_compact<W, T>(writer: W, value: &T) -> JsonResult<()>
where
    W: io::Write,
    T: ?Sized + Serialize,
{
    let mut ser = support::JoltSerializer::new(writer);
    value.serialize(ser.ser())
}

pub fn to_string_pretty_compact<T>(value: &T) -> JsonResult<String>
where
    T: ?Sized + Serialize,
{
    let mut writer = Vec::with_capacity(128);
    to_writer_pretty_compact(&mut writer, value)?;
    Ok(String::from_utf8(writer).expect("serde_json always produces valid UTF-8"))
}

mod support {
    use super::{io, PrettyCompactFormatter};

    pub struct JoltSerializer<W> {
        ser: serde_json::Serializer<W, PrettyCompactFormatter>,
    }

    #[cfg(test)]
    impl JoltSerializer<Vec<u8>> {
        pub fn new_vec() -> Self {
            let writer = Vec::with_capacity(128);
            Self::new(writer)
        }

        pub fn into_string(self) -> String {
            String::from_utf8(self.into_inner()).expect("serde_json always produces valid UTF-8")
        }
    }

    impl<W> JoltSerializer<W>
    where
        W: io::Write,
    {
        pub fn new(writer: W) -> Self {
            let ser = serde_json::Serializer::with_formatter(writer, PrettyCompactFormatter {});
            JoltSerializer { ser }
        }

        pub fn ser(&mut self) -> impl serde::Serializer<Ok = (), Error = serde_json::Error> + '_ {
            &mut self.ser
        }

        #[cfg(test)]
        pub fn into_inner(self) -> W {
            self.ser.into_inner()
        }
    }
}

#[cfg(test)]
pub mod support_pub {
    pub use super::support::*;
}
