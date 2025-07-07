use std::cell::LazyCell;
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;

use regex::Regex;

use crate::bolt_version::JoltVersion;
use crate::parse_error::ParseError;
use crate::values::bolt_struct::_parsing::{check_last_pack_stream_field, next_pack_stream_field};
use crate::values::bolt_struct::{TAG_POINT_2D, TAG_POINT_3D};
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

#[derive(Debug, Copy, Clone)]
pub(crate) struct JoltPoint {
    pub(crate) srid: i64,
    pub(crate) x: f64,
    pub(crate) y: f64,
    pub(crate) z: Option<f64>,
}

impl JoltPoint {
    pub(crate) fn new(srid: i64, x: f64, y: f64, z: Option<f64>) -> Self {
        Self { srid, x, y, z }
    }

    pub(crate) fn parse(s: &str) -> Result<Self, ParseError> {
        thread_local! {
            static SPATIAL_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(concat!(
                    r"^(?:SRID=([^;]+);)?\s*",
                    r"POINT\s*\(((?:(?:\S+)?\s+){1,2}\S+)\)$",
                )).unwrap()
            });
        }

        let captures = SPATIAL_RE.with(|re| re.captures(s));
        let Some(captures) = captures else {
            return Err(ParseError::new(format!(
                "Expected valid spatial string after sigil \"@\", \
                    e.g., \"SRID=7203;POINT(1 2)\" found: {s:?}"
            )));
        };
        let Some(srid_match) = captures.get(1) else {
            return Err(ParseError::new(format!(
                "Spatial string (after sigil \"@\") requires an SRID, \
                    e.g., \"SRID=7203;POINT(1 2)\" found: {s:?}"
            )));
        };
        let srid = srid_match.as_str();
        let srid = i64::from_str(srid).map_err(|e| {
            format!("Spatial string (after sigil \"@\") contained non-i64 srid {srid:?}): {e}")
        })?;
        let coords = &captures[2];
        let coords = coords
            .split_whitespace()
            .enumerate()
            .map(|(i, c)| {
                f64::from_str(c).map_err(|e| {
                    format!(
                        "Spatial string (after sigil \"@\") contained non-f64 coordinate \
                            {c:?} (at {i}): {e}"
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(match coords.as_slice() {
            [x, y] => JoltPoint::new(srid, *x, *y, None),
            [x, y, z] => JoltPoint::new(srid, *x, *y, Some(*z)),
            _ => unreachable!("Regex asserts exactly 2 or 3 coordinates"),
        })
    }

    pub(crate) fn as_struct(&self) -> PackStreamStruct {
        let mut fields = Vec::with_capacity(4);
        fields.extend([
            PackStreamValue::Integer(self.srid),
            PackStreamValue::Float(self.x),
            PackStreamValue::Float(self.y),
        ]);
        if let Some(z) = self.z {
            fields.push(PackStreamValue::Float(z))
        }
        let tag = match self.z {
            None => TAG_POINT_2D,
            Some(_) => TAG_POINT_3D,
        };
        PackStreamStruct { tag, fields }
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct BoltPoint(JoltPoint);

impl BoltPoint {
    pub(super) fn from_struct(s: &PackStreamStruct, _jolt_version: JoltVersion) -> Option<Self> {
        let PackStreamStruct { tag, fields } = s;
        if *tag != TAG_POINT_2D && *tag != TAG_POINT_3D {
            return None;
        }
        let mut fields = fields.iter();
        let srid = next_pack_stream_field(&mut fields)?;
        let x = next_pack_stream_field(&mut fields)?;
        let y = next_pack_stream_field(&mut fields)?;
        let z = match *tag {
            TAG_POINT_3D => Some(next_pack_stream_field(&mut fields)?),
            _ => None,
        };
        if !check_last_pack_stream_field(&mut fields) {
            return None;
        }
        Some(Self(JoltPoint::new(srid, x, y, z)))
    }

    pub(super) fn jolt_fmt(&self, _jolt_version: JoltVersion) -> impl Display + '_ {
        struct JoltFormatter<'a> {
            this: &'a BoltPoint,
        }

        impl Display for JoltFormatter<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.write_str(r#"{"@": "SRID="#)?;
                Display::fmt(&self.this.0.srid, f)?;
                f.write_str(";POINT(")?;
                Display::fmt(&self.this.0.x, f)?;
                f.write_str(" ")?;
                Display::fmt(&self.this.0.y, f)?;
                if let Some(z) = self.this.0.z {
                    f.write_str(" ")?;
                    Display::fmt(&z, f)?;
                }
                f.write_str(r#")"}"#)
            }
        }

        JoltFormatter { this: self }
    }
}
