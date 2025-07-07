use std::cell::LazyCell;
use std::fmt::{Debug, Display};
use std::str::FromStr;

use regex::{Captures, Regex};

use crate::bolt_version::JoltVersion;
use crate::parse_error::ParseError;
use crate::util::opt_res_ret;
use crate::values::bolt_struct::_common::normalize_seconds_nanos;
use crate::values::bolt_struct::_parsing::{check_last_pack_stream_field, next_pack_stream_field};
use crate::values::bolt_struct::TAG_DURATION;
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

#[derive(Debug, Copy, Clone)]
pub(crate) struct JoltDuration {
    pub(crate) months: i64,
    pub(crate) days: i64,
    pub(crate) seconds: i64,
    pub(crate) nanos: i64,
}

impl JoltDuration {
    pub(crate) fn parse(s: &str) -> Option<Result<Self, ParseError>> {
        thread_local! {
            static DURATION_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(concat!(
                    r"^P(?:(-?\d+)Y)?(?:(-?\d+)M)?(?:(-?\d+)D)",
                    r"(?:T(?:(-?\d+)H)?(?:(-?\d+)M)?(?:(-)?(\d+)(?:\.(\d{1,9}))?S)?)?"
                )).unwrap()
            });
        }
        let captures = DURATION_RE.with(|re| re.captures(s))?;
        let years = match i64::from_str(&captures[1]) {
            Ok(years) => years,
            Err(e) => {
                return Some(Err(ParseError::new(format!(
                    "Failed to parse duration years {}: {e}",
                    &captures[0]
                ))))
            }
        };

        fn i64_capture(
            i: usize,
            name: &str,
            captures: &Captures,
        ) -> Option<Result<i64, ParseError>> {
            Some(match captures.get(i) {
                None => Ok(0),
                Some(c) => i64::from_str(c.as_str()).map_err(|e| {
                    ParseError::new(format!(
                        "Failed to parse duration {name} {}: {e}",
                        c.as_str()
                    ))
                }),
            })
        }

        let months = opt_res_ret!(i64_capture(2, "months", &captures));
        let days = opt_res_ret!(i64_capture(3, "days", &captures));
        let hours = opt_res_ret!(i64_capture(4, "hours", &captures));
        let minutes = opt_res_ret!(i64_capture(5, "minutes", &captures));
        let seconds_sign = captures.get(6).map(|_| -1).unwrap_or(1);
        let mut seconds = opt_res_ret!(i64_capture(6, "seconds", &captures)) * seconds_sign;
        let mut nanos = captures
            .get(7)
            .map(|m| {
                // left align to fill in omitted decimal places
                i64::from_str(&format!("{:<09}", m.as_str()))
                    .expect("regex enforces nanos to be i64")
            })
            .unwrap_or_default();
        assert!(
            (0..=999_999_999).contains(&nanos),
            "regex enforces nanos to not overflow into seconds"
        );
        if seconds_sign < 0 && nanos > 0 {
            // sub-seconds being negative
            seconds -= 1;
            nanos = 1_000_000_000 - nanos;
        }

        let Some(months) = years
            .checked_mul(12)
            .and_then(|years_as_months| months.checked_add(years_as_months))
        else {
            return Some(Err(ParseError::new(
                "Duration months (together with years) are overflowing",
            )));
        };
        let Some(seconds) = hours
            .checked_mul(60)
            .and_then(|hours_as_minutes| minutes.checked_add(hours_as_minutes))
            .and_then(|minutes| seconds.checked_add(minutes * 60))
        else {
            return Some(Err(ParseError::new(
                "Duration seconds (together with hours and minutes) are overflowing",
            )));
        };
        Some(Ok(Self {
            months,
            days,
            seconds,
            nanos,
        }))
    }

    pub(crate) fn as_struct(&self) -> Option<Result<PackStreamStruct, ParseError>> {
        Some(Ok(PackStreamStruct {
            tag: TAG_DURATION,
            fields: vec![
                PackStreamValue::Integer(self.months),
                PackStreamValue::Integer(self.days),
                PackStreamValue::Integer(self.seconds),
                PackStreamValue::Integer(self.nanos),
            ],
        }))
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct BoltDuration(JoltDuration);

impl BoltDuration {
    pub(super) fn from_struct(s: &PackStreamStruct, _jolt_version: JoltVersion) -> Option<Self> {
        let PackStreamStruct { tag, fields } = s;
        if *tag != TAG_DURATION {
            return None;
        }
        let mut fields = fields.iter();
        let months = next_pack_stream_field(&mut fields)?;
        let days = next_pack_stream_field(&mut fields)?;
        let seconds = next_pack_stream_field(&mut fields)?;
        let nanos = next_pack_stream_field(&mut fields)?;
        let (seconds, nanos) = normalize_seconds_nanos(seconds, nanos)?;
        if !check_last_pack_stream_field(fields) {
            return None;
        }
        Some(Self(JoltDuration {
            months,
            days,
            seconds,
            nanos: nanos.into(),
        }))
    }

    pub(super) fn jolt_fmt(&self, _jolt_version: JoltVersion) -> impl Display + '_ {
        struct JoltFormatter<'a> {
            this: &'a BoltDuration,
        }

        fn write_component(
            f: &mut std::fmt::Formatter<'_>,
            n: i64,
            designator: &'_ str,
        ) -> std::fmt::Result {
            if n == 0 {
                Ok(())
            } else {
                Display::fmt(&n, f)?;
                f.write_str(designator)
            }
        }

        impl Display for JoltFormatter<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let BoltDuration(JoltDuration {
                    months,
                    days,
                    seconds,
                    mut nanos,
                }) = self.this;
                let (years, months) = (months / 12, months % 12);
                let (weeks, days) = (days / 7, days % 7);
                let (hours, seconds) = (seconds / 3600, seconds % 3600);
                let (minutes, mut seconds) = (seconds / 60, seconds % 60);
                if seconds < 0 {
                    seconds += 1;
                    nanos = 1_000_000_000 - nanos;
                }

                let has_time = hours != 0 || minutes != 0 || seconds != 0 || nanos != 0;
                let has_days = years != 0 || months != 0 || weeks != 0 || days != 0;

                f.write_str(r#"{"T": "P"#)?;
                write_component(f, years, "Y")?;
                write_component(f, months, "M")?;
                write_component(f, weeks, "W")?;
                write_component(f, days, "D")?;
                if has_time || !has_days {
                    f.write_str("T")?;
                    write_component(f, hours, "H")?;
                    write_component(f, minutes, "M")?;
                    if seconds != 0 || nanos != 0 || !has_time {
                        Display::fmt(&seconds, f)?;
                        if nanos != 0 {
                            f.write_str(".")?;
                            write!(f, "{nanos:09}")?;
                        }
                        f.write_str("S")?;
                    }
                }
                f.write_str(r#""}"#)
            }
        }

        JoltFormatter { this: self }
    }
}
