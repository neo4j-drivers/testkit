use std::cell::LazyCell;
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;

use regex::Regex;

use crate::bolt_version::JoltVersion;
use crate::jolt::JoltSigil;
use crate::parse_error::ParseError;
use crate::values::bolt_struct::_common::fmt_jolt_sigil_and_map;
use crate::values::bolt_struct::_parsing::{check_last_pack_stream_field, next_pack_stream_field};
use crate::values::bolt_struct::{TAG_POINT_2D, TAG_POINT_3D};
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

const SIGIL: &str = JoltSigil::Spatial.str();

#[derive(Debug, Copy, Clone)]
pub(crate) struct JoltPoint {
    pub(crate) srid: i64,
    pub(crate) x: f64,
    pub(crate) y: f64,
    pub(crate) z: Option<f64>,
    pub(super) jolt_version: JoltVersion,
}

#[cfg(test)]
fn float_eq(a: f64, b: f64) -> bool {
    a.to_bits() == b.to_bits() || a.is_nan() && b.is_nan()
}

#[cfg(test)]
impl PartialEq for JoltPoint {
    fn eq(&self, other: &Self) -> bool {
        self.srid == other.srid
            && float_eq(self.x, other.x)
            && float_eq(self.y, other.y)
            && match (self.z, other.z) {
                (None, None) => true,
                (Some(z1), Some(z2)) => float_eq(z1, z2),
                _ => false,
            }
    }
}

impl JoltPoint {
    pub(super) fn new(
        srid: i64,
        x: f64,
        y: f64,
        z: Option<f64>,
        jolt_version: JoltVersion,
    ) -> Self {
        Self {
            srid,
            x,
            y,
            z,
            jolt_version,
        }
    }

    pub(crate) fn parse(s: &str, jolt_version: JoltVersion) -> Result<Self, ParseError> {
        thread_local! {
            static SPATIAL_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(concat!(
                    r"^(?:SRID=([^;]*);)?\s*",
                    r"POINT\s*\(\s*((?:(?:\S+)?\s+){1,2}\S+\s*)\)$",
                )).unwrap()
            });
        }

        let captures = SPATIAL_RE.with(|re| re.captures(s));
        let Some(captures) = captures else {
            return Err(ParseError::new(format!(
                "Expected valid spatial string after sigil \"{SIGIL}\", \
                e.g., \"SRID=7203;POINT(1 2)\" found: {s:?}"
            )));
        };
        let Some(srid_match) = captures.get(1) else {
            return Err(ParseError::new(format!(
                "Spatial string (after sigil \"{SIGIL}\") requires an SRID, \
                e.g., \"SRID=7203;POINT(1 2)\" found: {s:?}"
            )));
        };
        let srid = srid_match.as_str().trim();
        let srid = i64::from_str(srid).map_err(|e| {
            format!(
                "Spatial string (after sigil \"{SIGIL}\") \
                contained non-i64 srid {srid:?}): {e}"
            )
        })?;
        let coords = &captures[2];
        let coords = coords
            .split_whitespace()
            .enumerate()
            .map(|(i, c)| {
                f64::from_str(c).map_err(|e| {
                    format!(
                        "Spatial string (after sigil \"{SIGIL}\") \
                        contained non-f64 coordinate {c:?} (at {i}): {e}"
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(match coords.as_slice() {
            [x, y] => JoltPoint::new(srid, *x, *y, None, jolt_version),
            [x, y, z] => JoltPoint::new(srid, *x, *y, Some(*z), jolt_version),
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
            fields.push(PackStreamValue::Float(z));
        }
        let tag = match self.z {
            None => TAG_POINT_2D,
            Some(_) => TAG_POINT_3D,
        };
        PackStreamStruct { tag, fields }
    }

    pub(super) fn from_struct(s: &PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
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
        Some(JoltPoint::new(srid, x, y, z, jolt_version))
    }

    pub(super) fn jolt_fmt(&self, jolt_version: JoltVersion) -> impl Display + '_ {
        struct JoltFormatter<'a> {
            data: &'a JoltPoint,
            jolt_version: JoltVersion,
        }

        impl Display for JoltFormatter<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                fmt_jolt_sigil_and_map(
                    f,
                    SIGIL,
                    self.data.jolt_version,
                    self.jolt_version,
                    Some(("\"", "\"")),
                    |f| {
                        f.write_str("SRID=")?;
                        Display::fmt(&self.data.srid, f)?;
                        f.write_str(";POINT(")?;
                        Display::fmt(&self.data.x, f)?;
                        f.write_str(" ")?;
                        Display::fmt(&self.data.y, f)?;
                        if let Some(z) = self.data.z {
                            f.write_str(" ")?;
                            Display::fmt(&z, f)?;
                        }
                        f.write_str(")")
                    },
                )
            }
        }

        JoltFormatter {
            data: self,
            jolt_version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;

    #[rstest]
    #[case("SRID=0;POINT(0 0)", 0, 0.0, 0.0, None)]
    #[case("SRID=0;POINT(0 0 0)", 0, 0.0, 0.0, Some(0.0))]
    #[case("SRID=0 ;  POINT( 0   0  )", 0, 0.0, 0.0, None)]
    #[case("SRID=0 ;  POINT( 0   0    \t 0 )", 0, 0.0, 0.0, Some(0.0))]
    #[case("SRID=1;POINT(2 3)", 1, 2.0, 3.0, None)]
    #[case("SRID=1;POINT(2 3 4)", 1, 2.0, 3.0, Some(4.0))]
    #[case("SRID=4326;POINT(56.21 13.43)", 4326, 56.21, 13.43, None)]
    #[case(
        "SRID=4326;POINT(56.21 13.43 42.1337)",
        4326,
        56.21,
        13.43,
        Some(42.1337)
    )]
    #[case("SRID=-1;POINT(-2 -3 -4)", -1, -2.0, -3.0, Some(-4.0))]
    #[case("SRID=-1234;POINT(-2.1 -3.2 -4.3)", -1234,-2.1 ,-3.2,Some(-4.3))]
    #[case("SRID=0;POINT(NaN -NaN +NaN)", 0, f64::NAN, -f64::NAN, Some(f64::NAN))]
    #[case("SRID=0;POINT(-infinity +infinity)", 0, -f64::INFINITY, f64::INFINITY, None)]
    #[case("SRID=0;POINT(-inf +inf)", 0, -f64::INFINITY, f64::INFINITY, None)]
    fn test_parse(
        #[case] input: &str,
        #[case] srid: i64,
        #[case] x: f64,
        #[case] y: f64,
        #[case] z: Option<f64>,
        #[values(JoltVersion::V1, JoltVersion::V2, JoltVersion::V3, JoltVersion::V4)]
        jolt_version: JoltVersion,
    ) {
        let point = JoltPoint::parse(input, jolt_version).expect("failed to parse");

        let expected = JoltPoint::new(srid, x, y, z, jolt_version);
        assert_eq!(point, expected);
    }

    #[rstest]
    #[case("POINT(0 0)")]
    #[case("SRID;POINT(0 0)")]
    #[case("SRID=;POINT(0 0)")]
    #[case("SRID=a;POINT(0 0)")]
    #[case("SRID=NaN;POINT(0 0)")]
    #[case("SRID=inf;POINT(0 0)")]
    #[case("SRID=0.0;POINT(0 0)")]
    #[case("SRID=0.;POINT(0 0)")]
    #[case("SRID=0;POINT(0, 0)")]
    #[case("SRID=0;POINT(0)")]
    #[case("SRID=0;POINT(0 0 0 0)")]
    #[case("SRID=0;(0 0)")]
    fn test_jolt_point_parse_invalid(
        #[case] input: &str,
        #[values(JoltVersion::V1, JoltVersion::V2, JoltVersion::V3, JoltVersion::V4)]
          jolt_version: JoltVersion,
    ) {
        let res = JoltPoint::parse(input, jolt_version);

        res.expect_err("parsing succeededs");
    }

    #[rstest]
    #[case("SRID=0;POINT(0 0)", 0, 0.0, 0.0, None)]
    #[case("SRID=0;POINT(0 0 0)", 0, 0.0, 0.0, Some(0.0))]
    #[case("SRID=1;POINT(2 3)", 1, 2.0, 3.0, None)]
    #[case("SRID=1;POINT(2 3 4)", 1, 2.0, 3.0, Some(4.0))]
    #[case("SRID=4326;POINT(56.21 13.43)", 4326, 56.21, 13.43, None)]
    #[case(
        "SRID=4326;POINT(56.21 13.43 42.1337)",
        4326,
        56.21,
        13.43,
        Some(42.1337)
    )]
    #[case("SRID=-1;POINT(-2 -3 -4)", -1, -2.0, -3.0, Some(-4.0))]
    #[case("SRID=-1234;POINT(-2.1 -3.2 -4.3)", -1234,-2.1 ,-3.2,Some(-4.3))]
    #[case("SRID=0;POINT(NaN NaN NaN)", 0, f64::NAN, -f64::NAN, Some(f64::NAN))]
    #[case("SRID=0;POINT(-inf inf)", 0, -f64::INFINITY, f64::INFINITY, None)]
    fn test_jolt_fmt(
        #[case] expected: &str,
        #[case] srid: i64,
        #[case] x: f64,
        #[case] y: f64,
        #[case] z: Option<f64>,
        #[values(JoltVersion::V1, JoltVersion::V2, JoltVersion::V3, JoltVersion::V4)]
        jolt_version: JoltVersion,
    ) {
        let point = JoltPoint::new(srid, x, y, z, jolt_version);

        let formatted = point.jolt_fmt(jolt_version).to_string();

        let expected = format!(r#"{{"@": "{expected}"}}"#);
        assert_eq!(formatted, expected);
    }

    #[rstest]
    #[case(0, 0.0, 0.0, None)]
    #[case(0, 0.0, 0.0, Some(0.0))]
    #[case(1, 2.0, 3.0, None)]
    #[case(1, 2.0, 3.0, Some(4.0))]
    #[case(4326, 56.21, 13.43, None)]
    #[case(4326, 56.21, 13.43, Some(42.1337))]
    #[case(-1, -2.0, -3.0, Some(-4.0))]
    #[case(-1234,-2.1 ,-3.2,Some(-4.3))]
    #[case(0, f64::NAN, -f64::NAN, Some(f64::NAN))]
    #[case(0, -f64::INFINITY, f64::INFINITY, None)]
    fn test_to_struct(
        #[case] srid: i64,
        #[case] x: f64,
        #[case] y: f64,
        #[case] z: Option<f64>,
        #[values(JoltVersion::V1, JoltVersion::V2, JoltVersion::V3, JoltVersion::V4)]
        jolt_version: JoltVersion,
    ) {
        let point = JoltPoint::new(srid, x, y, z, jolt_version);

        let struct_ = point.as_struct();

        let (tag, fields) = match z {
            Some(z) => {
                let fields = vec![srid.into(), x.into(), y.into(), z.into()];
                (TAG_POINT_3D, fields)
            }
            None => {
                let fields = vec![srid.into(), x.into(), y.into()];
                (TAG_POINT_2D, fields)
            }
        };
        let expected = PackStreamStruct { tag, fields };
        assert_eq!(struct_, expected);
    }

    #[rstest]
    #[case(0, 0.0, 0.0, None)]
    #[case(0, 0.0, 0.0, Some(0.0))]
    #[case(1, 2.0, 3.0, None)]
    #[case(1, 2.0, 3.0, Some(4.0))]
    #[case(4326, 56.21, 13.43, None)]
    #[case(4326, 56.21, 13.43, Some(42.1337))]
    #[case(-1, -2.0, -3.0, Some(-4.0))]
    #[case(-1234,-2.1 ,-3.2,Some(-4.3))]
    #[case(0, f64::NAN, -f64::NAN, Some(f64::NAN))]
    #[case(0, -f64::INFINITY, f64::INFINITY, None)]
    fn test_from_to_struct(
        #[case] srid: i64,
        #[case] x: f64,
        #[case] y: f64,
        #[case] z: Option<f64>,
        #[values(JoltVersion::V1, JoltVersion::V2, JoltVersion::V3, JoltVersion::V4)]
        jolt_version: JoltVersion,
    ) {
        let (tag, fields) = match z {
            Some(z) => {
                let fields = vec![srid.into(), x.into(), y.into(), z.into()];
                (TAG_POINT_3D, fields)
            }
            None => {
                let fields = vec![srid.into(), x.into(), y.into()];
                (TAG_POINT_2D, fields)
            }
        };
        let struct_ = PackStreamStruct { tag, fields };

        let point = JoltPoint::from_struct(&struct_, jolt_version).expect("failed to load");

        let expected = JoltPoint::new(srid, x, y, z, jolt_version);
        assert_eq!(point, expected);
    }
}
