use std::cell::LazyCell;
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;

use chrono::{NaiveTime, Timelike};
use regex::Regex;

use crate::bolt_version::JoltVersion;
use crate::parse_error::ParseError;
use crate::values::bolt_struct::_parsing::{check_last_pack_stream_field, next_pack_stream_field};
use crate::values::bolt_struct::{TAG_LOCAL_TIME, TAG_TIME};
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

#[derive(Debug)]
pub(crate) struct JoltTime<'a> {
    pub(crate) time: NaiveTime,
    pub(crate) utc_offset_seconds: Option<i32>,
    pub(crate) time_zone_id: Option<&'a str>,
}

impl<'a> JoltTime<'a> {
    pub(crate) fn parse(s: &'a str) -> Option<Result<Self, ParseError>> {
        thread_local! {
            static TIME_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(concat!(
                    r"^(\d{2}):(\d{2})(?::(\d{2}))?(?:\.(\d{1,9}))?",
                    r"(?:(Z)|(?:(\+00(?::?00)?|[+-]00:?(?:[0-5][1-9]|[1-5]\d)|",
                    r"(?:[+-](?:0[1-9]|1\d|2[0-3]):?[0-5]\d)))(?:\[([^\]]+)\])?)?$"
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
        let minutes = captures
            .get(2)
            .map(|m| u32::from_str(m.as_str()).expect("regex enforces minutes to be u32"))
            .unwrap_or_default();
        let seconds = captures
            .get(3)
            .map(|m| u32::from_str(m.as_str()).expect("regex enforces seconds to be u32"))
            .unwrap_or_default();
        if seconds >= 60 {
            return Some(Err(ParseError::new(format!(
                "Unparsable time: {s:?} (leap seconds are not supported)"
            ))));
        }
        let nanos = captures
            .get(4)
            .map(|m| {
                // left align to fill in omitted decimal places
                u32::from_str(&format!("{:<09}", m.as_str()))
                    .expect("regex enforces nanos to be u32")
            })
            .unwrap_or_default();
        let utc_offset_seconds = match captures.get(5) {
            None => captures.get(6).map(|m| {
                // ±XY[[:]ZT]
                let s = m.as_str();
                let sign = match &s[..1] {
                    "+" => 1,
                    "-" => -1,
                    _ => unreachable!("regex enforces offset sign to be + or -"),
                };
                let hours =
                    i32::from_str(&s[1..=2]).expect("regex enforces offset hours to be i32");
                let minutes = match s.len() {
                    3 => 0,
                    5 => i32::from_str(&s[3..=4]).expect("regex enforces offset minutes to be i32"),
                    6 => i32::from_str(&s[4..=5]).expect("regex enforces offset minutes to be i32"),
                    _ => unreachable!(
                        "regex enforces offset to have length 3, 5, or six, found {s:?}"
                    ),
                };
                sign * (hours * 60 + minutes) * 60
            }),
            Some(_) => Some(0), // Z
        };
        let time_zone_id = captures.get(7).map(|m| m.as_str());
        let Some(time) = NaiveTime::from_hms_nano_opt(hours, minutes, seconds, nanos) else {
            return Some(Err(ParseError::new(format!("Unparsable time: {s:?}"))));
        };
        Some(Ok(Self {
            time,
            utc_offset_seconds,
            time_zone_id,
        }))
    }

    pub(crate) fn as_struct(&self) -> Option<Result<PackStreamStruct, ParseError>> {
        if self.time_zone_id.is_some() {
            // times only accept UTC offsets, not time zone ids
            return None;
        }
        let nanos_since_midnight = i64::from(self.time.num_seconds_from_midnight()) * 1_000_000_000
            + i64::from(self.time.nanosecond());
        Some(Ok(match self.utc_offset_seconds {
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
                    PackStreamValue::Integer(offset.into()),
                ],
            },
        }))
    }
}

#[derive(Debug)]
pub(super) struct BoltTime<'a> {
    pub(super) time: NaiveTime,
    pub(super) utc_offset_seconds: Option<i64>,
    pub(super) time_zone_id: Option<&'a str>,
}

impl<'a> BoltTime<'a> {
    pub(super) fn from_struct(s: &'a PackStreamStruct, _jolt_version: JoltVersion) -> Option<Self> {
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

        Some(Self {
            time,
            utc_offset_seconds,
            time_zone_id: None,
        })
    }

    pub(super) fn jolt_fmt(&self, _jolt_version: JoltVersion) -> impl Display + '_ {
        struct JoltFormatter<'a> {
            this: &'a BoltTime<'a>,
        }

        impl Display for JoltFormatter<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.write_str(r#"{"T": ""#)?;
                self.this.jolt_fmt_innder(f)?;
                f.write_str(r#""}"#)
            }
        }

        JoltFormatter { this: self }
    }

    pub(super) fn jolt_fmt_innder(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.time.format("%H:%M:%S%.f"), f)?;
        if let Some(utc_offset_seconds) = self.utc_offset_seconds {
            if utc_offset_seconds == 0 {
                return f.write_str("Z");
            } else {
                let sign = if utc_offset_seconds < 0 { "-" } else { "+" };
                let utc_offset_seconds = utc_offset_seconds.abs();
                let utc_offset_hours = utc_offset_seconds / (60 * 60);
                let utc_offset_minutes = (utc_offset_hours % (60 * 60)) / 60;
                let utc_offset_seconds = utc_offset_seconds % 60;
                write!(f, "{sign}{:02}:{:02}", utc_offset_hours, utc_offset_minutes)?;
                if utc_offset_seconds != 0 {
                    // not actually ISO compliant :/
                    write!(f, ":{:02}", utc_offset_seconds)?;
                }
            }
        }
        if let Some(zone_id) = &self.time_zone_id {
            f.write_str("[")?;
            f.write_str(zone_id)?;
            f.write_str("]")?;
        }
        Ok(())
    }
}
