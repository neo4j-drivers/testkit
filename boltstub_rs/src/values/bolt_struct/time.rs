use std::cell::LazyCell;
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;

use chrono::{NaiveTime, Timelike};
use regex::Regex;

use crate::bolt_version::JoltVersion;
use crate::jolt::JoltSigil;
use crate::parse_error::ParseError;
use crate::util::opt_res_ret;
use crate::values::bolt_struct::_common::fmt_jolt_sigil_and_map;
use crate::values::bolt_struct::_parsing::{check_last_pack_stream_field, next_pack_stream_field};
use crate::values::bolt_struct::{TAG_LOCAL_TIME, TAG_TIME};
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

const SIGIL: &str = JoltSigil::Temporal.str();

#[derive(Debug, Clone, Copy)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct JoltTimeData<'a> {
    pub(crate) time: NaiveTime,
    pub(crate) utc_offset_seconds: Option<i64>,
    pub(crate) time_zone_id: Option<&'a str>,
}

impl<'a> JoltTimeData<'a> {
    pub(super) fn parse(
        s: &'a str,
        _jolt_version: JoltVersion,
    ) -> Option<Result<Self, ParseError>> {
        thread_local! {
            static TIME_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(concat!(
                    r"^(\d{2}):(\d{2})(?::(\d{2}))?(?:\.(\d+))?",
                    r"(Z|([+-])(\d{2})(?::?(\d{2})(?::?(\d{2}))?)?)?(?:\[([^\]]+)\])?$",
                )).unwrap()
            });
        }
        let captures = TIME_RE.with(|re| re.captures(s))?;
        let hours = u32::from_str(
            captures
                .get(1)
                .expect("regex enforces existence of hours")
                .as_str(),
        )
        .expect("regex enforces hours to be u32");
        opt_res_ret!(Some(Self::check_bounds(hours, 0..=23, "hours", s)));
        let minutes = captures
            .get(2)
            .map(|m| u32::from_str(m.as_str()).expect("regex enforces minutes to be u32"))
            .unwrap_or_default();
        opt_res_ret!(Some(Self::check_bounds(minutes, 0..=59, "minutes", s)));
        let seconds = captures
            .get(3)
            .map(|m| u32::from_str(m.as_str()).expect("regex enforces seconds to be u32"))
            .unwrap_or_default();
        opt_res_ret!(Some(Self::check_bounds(seconds, 0..=59, "seconds", s)));
        let nanos = opt_res_ret!(captures.get(4).map_or(Some(Ok(0)), |m| {
            let padded = format!("{:0<9}", m.as_str());
            if padded.len() > 9 {
                return Some(Err(ParseError::new(
                    "Time has too many sub-second digits \
                    (only nanoseconds are allowed, up to 9 digits)",
                )));
            }
            Some(Ok(
                u32::from_str(&padded).expect("regex + length check enforce nanos to be u32")
            ))
        }));
        let utc_offset_seconds = match captures.get(5).map(|g| g.as_str()) {
            None => None,
            Some("Z") => Some(0),
            Some(_) => {
                let sign = match captures
                    .get(6)
                    .expect("regex enforces presence of sign")
                    .as_str()
                {
                    "+" => 1,
                    "-" => -1,
                    _ => unreachable!("regex enforces offset sign to be + or -"),
                };
                let hours = captures
                    .get(7)
                    .map(|g| {
                        i64::from_str(g.as_str()).expect("regex enforces offset hours to be i64")
                    })
                    .unwrap_or_default();
                let minutes = captures
                    .get(8)
                    .map(|g| {
                        i64::from_str(g.as_str()).expect("regex enforces offset minutes to be i64")
                    })
                    .unwrap_or_default();
                let seconds = captures
                    .get(9)
                    .map(|g| {
                        i64::from_str(g.as_str()).expect("regex enforces offset seconds to be i64")
                    })
                    .unwrap_or_default();
                Some(sign * (((hours * 60) + minutes) * 60 + seconds))
            }
        };
        if let Some(utc_offset_seconds) = utc_offset_seconds {
            const MAX_OFFSET: i64 = 24 * 60 * 60;
            opt_res_ret!(Some(Self::check_bounds(
                utc_offset_seconds,
                -MAX_OFFSET..=MAX_OFFSET,
                "utc_offset_seconds",
                s
            )));
        }
        let time_zone_id = captures.get(10).map(|m| m.as_str());
        let Some(time) = NaiveTime::from_hms_nano_opt(hours, minutes, seconds, nanos) else {
            return Some(Err(ParseError::new(format!("Unparsable time: {s:?}"))));
        };
        Some(Ok(Self {
            time,
            utc_offset_seconds,
            time_zone_id,
        }))
    }

    fn check_bounds<T: PartialOrd + Display + Debug + Copy>(
        x: T,
        bounds: std::ops::RangeInclusive<T>,
        name: &str,
        full: &str,
    ) -> Result<(), ParseError> {
        if !bounds.contains(&x) {
            return Err(ParseError::new(format!(
                "Unparsable time {full}: {x} is out of bounds for {name} (expected {bounds:?})",
            )));
        }
        Ok(())
    }

    pub(super) fn jolt_fmt(
        &self,
        jolt_version_data: JoltVersion,
        jolt_version_ctx: JoltVersion,
    ) -> impl Display + '_ {
        struct JoltFormatter<'a> {
            data: &'a JoltTimeData<'a>,
            jolt_version_data: JoltVersion,
            jolt_version_ctx: JoltVersion,
        }

        impl Display for JoltFormatter<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                fmt_jolt_sigil_and_map(
                    f,
                    SIGIL,
                    self.jolt_version_data,
                    self.jolt_version_ctx,
                    Some(("\"", "\"")),
                    |f| self.data.repr(f),
                )
            }
        }

        JoltFormatter {
            data: self,
            jolt_version_data,
            jolt_version_ctx,
        }
    }

    pub(super) fn repr(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fn repr_offset(
            f: &mut Formatter<'_>,
            utc_offset_seconds: Option<i64>,
            allow_z: bool,
        ) -> std::fmt::Result {
            let Some(utc_offset_seconds) = utc_offset_seconds else {
                return Ok(());
            };
            if allow_z && utc_offset_seconds == 0 {
                return f.write_str("Z");
            }
            let sign = if utc_offset_seconds < 0 { "-" } else { "+" };
            let utc_offset_seconds = utc_offset_seconds.abs();
            let utc_offset_hours = utc_offset_seconds / (60 * 60);
            let utc_offset_minutes = (utc_offset_seconds % (60 * 60)) / 60;
            let utc_offset_seconds = utc_offset_seconds % 60;
            write!(f, "{sign}{utc_offset_hours:02}:{utc_offset_minutes:02}")?;
            if utc_offset_seconds != 0 {
                // not actually ISO compliant :/
                write!(f, ":{utc_offset_seconds:02}")?;
            }
            Ok(())
        }
        fn repr_zone_id(f: &mut Formatter<'_>, zone_id: Option<&str>) -> std::fmt::Result {
            let Some(zone_id) = zone_id else {
                return Ok(());
            };
            f.write_str("[")?;
            f.write_str(zone_id)?;
            f.write_str("]")
        }

        Display::fmt(&self.time.format("%H:%M:%S%.f"), f)?;
        repr_offset(f, self.utc_offset_seconds, self.time_zone_id.is_none())?;
        repr_zone_id(f, self.time_zone_id)
    }
}
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct JoltTime<'a> {
    pub(crate) data: JoltTimeData<'a>,
    pub(crate) jolt_version: JoltVersion,
}

impl<'a> JoltTime<'a> {
    pub(crate) fn parse(s: &'a str, jolt_version: JoltVersion) -> Option<Result<Self, ParseError>> {
        JoltTimeData::parse(s, jolt_version).map(|res| {
            res.and_then(|data| {
                if data.time_zone_id.is_some() {
                    return Err(ParseError::new(
                        "Times only accept UTC offsets, not time zone ids",
                    ));
                }
                Ok(Self { data, jolt_version })
            })
        })
    }

    #[expect(clippy::unnecessary_wraps, reason = "consistent API with other types")]
    pub(crate) fn as_struct(&self) -> Option<Result<PackStreamStruct, ParseError>> {
        assert!(
            self.data.time_zone_id.is_none(),
            "times only accept UTC offsets, not time zone ids"
        );
        let nanos_since_midnight = i64::from(self.data.time.num_seconds_from_midnight())
            * 1_000_000_000
            + i64::from(self.data.time.nanosecond());
        Some(Ok(match self.data.utc_offset_seconds {
            // local time
            None => PackStreamStruct {
                tag: TAG_LOCAL_TIME,
                fields: vec![PackStreamValue::Integer(nanos_since_midnight)],
            },
            // time
            Some(offset) => PackStreamStruct {
                tag: TAG_TIME,
                fields: vec![
                    PackStreamValue::Integer(nanos_since_midnight),
                    PackStreamValue::Integer(offset),
                ],
            },
        }))
    }

    pub(super) fn from_struct(s: &'a PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        let PackStreamStruct { tag, fields } = s;
        let mut fields = fields.iter();
        if *tag != TAG_LOCAL_TIME && *tag != TAG_TIME {
            return None;
        }
        let nanos_since_midnight: i64 = next_pack_stream_field(&mut fields)?;
        let (seconds, nanos) = (
            nanos_since_midnight / 1_000_000_000,
            nanos_since_midnight % 1_000_000_000,
        );
        let seconds = seconds.try_into().ok()?;
        let nanos = nanos.try_into().ok()?;
        let time = NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanos)?;
        let utc_offset_seconds = match *tag {
            TAG_TIME => Some(next_pack_stream_field(&mut fields)?),
            _ => None,
        };
        if !check_last_pack_stream_field(&mut fields) {
            return None;
        }
        let data = JoltTimeData {
            time,
            utc_offset_seconds,
            time_zone_id: None,
        };

        Some(Self { data, jolt_version })
    }

    pub(super) fn jolt_fmt(&self, jolt_version: JoltVersion) -> impl Display + '_ {
        self.data.jolt_fmt(self.jolt_version, jolt_version)
    }
}
