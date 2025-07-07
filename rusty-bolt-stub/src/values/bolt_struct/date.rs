use std::cell::LazyCell;
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;

use chrono::{Duration, NaiveDate};
use regex::Regex;

use crate::bolt_version::JoltVersion;
use crate::parse_error::ParseError;
use crate::values::bolt_struct::_parsing::{check_last_pack_stream_field, next_pack_stream_field};
use crate::values::bolt_struct::TAG_DATE;
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

const UNIX_EPOCH_DATE: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

#[derive(Debug, Copy, Clone)]
pub(crate) struct JoltDate {
    pub(crate) date: NaiveDate,
}

impl JoltDate {
    pub(crate) fn parse(s: &str) -> Option<Result<Self, ParseError>> {
        thread_local! {
            static DATE_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(r"^(\d{4})(?:-(\d{2}))?(?:-(\d{2}))?$").unwrap()
            });
        }
        let captures = DATE_RE.with(|re| re.captures(s))?;
        let year = i32::from_str(
            captures
                .get(1)
                .expect("regex enforces existence of year")
                .as_str(),
        )
        .expect("regex enforces year to be i32");
        let month = captures
            .get(2)
            .map(|m| u32::from_str(m.as_str()).expect("regex enforces month to be u32"))
            .unwrap_or(1);
        let day = captures
            .get(3)
            .map(|d| u32::from_str(d.as_str()).expect("regex enforces day to be u32"))
            .unwrap_or(1);
        let Some(date) = NaiveDate::from_ymd_opt(year, month, day) else {
            return Some(Err(ParseError::new(format!("Unparsable date {s:?}"))));
        };
        Some(Ok(Self { date }))
    }

    pub(crate) fn as_struct(&self) -> Option<Result<PackStreamStruct, ParseError>> {
        let days_since_epoch = self.date - UNIX_EPOCH_DATE;
        Some(Ok(PackStreamStruct {
            tag: TAG_DATE,
            fields: vec![PackStreamValue::Integer(days_since_epoch.num_days())],
        }))
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct BoltDate(pub(super) JoltDate);

impl BoltDate {
    pub(super) fn from_struct(s: &PackStreamStruct, _jolt_version: JoltVersion) -> Option<Self> {
        if s.tag != TAG_DATE {
            return None;
        }
        let mut fields = s.fields.iter();
        let days_since_epoch: i64 = next_pack_stream_field(&mut fields)?;
        let date = UNIX_EPOCH_DATE + Duration::days(days_since_epoch);
        if !check_last_pack_stream_field(&mut fields) {
            return None;
        }

        Some(Self(JoltDate { date }))
    }

    pub(super) fn jolt_fmt(&self, _jolt_version: JoltVersion) -> impl Display + '_ {
        struct JoltFormatter<'a> {
            this: &'a BoltDate,
        }

        impl Display for JoltFormatter<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.write_str(r#"{"T": ""#)?;
                self.this.repr_inner(f)?;
                f.write_str(r#""}"#)
            }
        }

        JoltFormatter { this: self }
    }

    pub(super) fn repr_inner(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0.date.format("%Y-%m-%d"), f)
    }
}
