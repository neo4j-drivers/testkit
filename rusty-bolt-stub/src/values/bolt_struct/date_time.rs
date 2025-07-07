use std::cell::LazyCell;
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;

use chrono::{
    DateTime, Duration, FixedOffset, MappedLocalTime, NaiveDateTime, Offset, TimeDelta, TimeZone,
    Utc,
};
use chrono_tz::Tz;
use regex::Regex;

use crate::bolt_version::JoltVersion;
use crate::parse_error::ParseError;
use crate::util::opt_res_ret;
use crate::values::bolt_struct::_common::normalize_seconds_nanos;
use crate::values::bolt_struct::_parsing::{check_last_pack_stream_field, next_pack_stream_field};
use crate::values::bolt_struct::date::BoltDate;
use crate::values::bolt_struct::time::BoltTime;
use crate::values::bolt_struct::{
    JoltDate, JoltTime, TAG_DATE_TIME_V1, TAG_DATE_TIME_V2, TAG_DATE_TIME_ZONE_ID_V1,
    TAG_DATE_TIME_ZONE_ID_V2, TAG_LOCAL_DATE_TIME,
};
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

const UNIX_EPOCH_DATE_TIME: DateTime<Utc> = DateTime::from_timestamp(0, 0).unwrap();

#[derive(Debug)]
pub(crate) struct JoltDateTime<'a> {
    pub(crate) date: JoltDate,
    pub(crate) time: JoltTime<'a>,
}

impl<'a> JoltDateTime<'a> {
    pub(crate) fn parse(s: &'a str) -> Option<Result<Self, ParseError>> {
        thread_local! {
            static DATE_TIME_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(r"^(.*?)T(.*)$").unwrap()
            });
        }
        let captures = DATE_TIME_RE.with(|re| re.captures(s))?;
        let date = opt_res_ret!(JoltDate::parse(&captures[1]));
        let time = opt_res_ret!(JoltTime::parse(captures.get(2).unwrap().as_str()));
        Some(Ok(Self { date, time }))
    }

    pub(crate) fn into_struct(
        self,
        jolt_version: JoltVersion,
    ) -> Option<Result<PackStreamStruct, ParseError>> {
        let JoltDate { date } = self.date;
        let JoltTime {
            time,
            utc_offset_seconds,
            time_zone_id,
        } = self.time;
        let date_time = NaiveDateTime::new(date, time);
        Some(Ok(match (utc_offset_seconds, time_zone_id) {
            (Some(utc_offset_seconds), Some(time_zone_id)) => {
                // date time zone id

                // let tz = match Tz::from_str(time_zone_id) {
                //     Ok(tz) => tz,
                //     Err(e) => {
                //         return Some(Err(ParseError::new(format!(
                //             "Failed to load time zone id {time_zone_id:?}: {e}"
                //         ))))
                //     }
                // };
                // let date_time = date_time_utc.with_timezone(&tz);
                // let found_offset_seconds = date_time.offset().fix().local_minus_utc();
                // if found_offset_seconds != utc_offset_seconds {
                //     // "Timezone database for {s} does not agree with the offset \
                //     // {utc_offset_seconds} seconds, found {found_offset_seconds}. \
                //     // Either there is a typo or the timezone database is outdated."
                //     todo!("Emit warning, possibly a typo in the script");
                // }
                match jolt_version {
                    JoltVersion::V1 => {
                        let date_time = date_time.and_utc();
                        let seconds = date_time.timestamp();
                        let nanos = date_time.timestamp_subsec_nanos();
                        PackStreamStruct {
                            tag: TAG_DATE_TIME_ZONE_ID_V1,
                            fields: vec![
                                PackStreamValue::Integer(seconds),
                                PackStreamValue::Integer(nanos.into()),
                                PackStreamValue::String(String::from(time_zone_id)),
                            ],
                        }
                    }
                    JoltVersion::V2 => {
                        let date_time_utc = date_time.and_utc()
                            - TimeDelta::new(utc_offset_seconds.into(), 0).unwrap();
                        let seconds = date_time_utc.timestamp();
                        let nanos = date_time_utc.timestamp_subsec_nanos();
                        PackStreamStruct {
                            tag: TAG_DATE_TIME_ZONE_ID_V2,
                            fields: vec![
                                PackStreamValue::Integer(seconds),
                                PackStreamValue::Integer(nanos.into()),
                                PackStreamValue::String(String::from(time_zone_id)),
                            ],
                        }
                    }
                }
            }
            (Some(utc_offset_seconds), None) => {
                // date time
                match jolt_version {
                    JoltVersion::V1 => {
                        let date_time = date_time.and_utc();
                        let seconds = date_time.timestamp();
                        let nanos = date_time.timestamp_subsec_nanos();
                        PackStreamStruct {
                            tag: TAG_DATE_TIME_V1,
                            fields: vec![
                                PackStreamValue::Integer(seconds),
                                PackStreamValue::Integer(nanos.into()),
                                PackStreamValue::Integer(utc_offset_seconds.into()),
                            ],
                        }
                    }
                    JoltVersion::V2 => {
                        let tz = FixedOffset::east_opt(utc_offset_seconds)
                            .expect("regex enforced offset in bounds");
                        let date_time = date_time.and_local_timezone(tz).unwrap();
                        let seconds = date_time.timestamp();
                        let nanos = date_time.timestamp_subsec_nanos();
                        PackStreamStruct {
                            tag: TAG_DATE_TIME_V2,
                            fields: vec![
                                PackStreamValue::Integer(seconds),
                                PackStreamValue::Integer(nanos.into()),
                                PackStreamValue::Integer(utc_offset_seconds.into()),
                            ],
                        }
                    }
                }
            }
            (None, Some(_)) => return Some(Err(ParseError::new(
                "DateTime with named zone requires an explicit time offset to avoid ambiguity. \
                E.g., `2025-03-06T18:05:02+01:00[Europe/Stockholm]` instead of \
                `2025-03-06T18:05:02[Europe/Stockholm]`",
            ))),
            (None, None) => {
                // local date time
                let date_time = date_time.and_utc();
                let seconds = date_time.timestamp();
                let nanos = date_time.timestamp_subsec_nanos();
                PackStreamStruct {
                    tag: TAG_LOCAL_DATE_TIME,
                    fields: vec![
                        PackStreamValue::Integer(seconds),
                        PackStreamValue::Integer(nanos.into()),
                    ],
                }
            }
        }))
    }
}

#[derive(Debug)]
pub(super) struct BoltDateTime<'a> {
    pub(super) date: BoltDate,
    pub(super) time: BoltTime<'a>,
}

impl<'a> BoltDateTime<'a> {
    pub(super) fn from_struct(s: &'a PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        fn new(
            date_time: NaiveDateTime,
            utc_offset_seconds: Option<i64>,
            time_zone_id: Option<&str>,
        ) -> BoltDateTime {
            let date = date_time.date();
            let time = date_time.time();
            BoltDateTime {
                date: BoltDate(JoltDate { date }),
                time: BoltTime {
                    time,
                    utc_offset_seconds,
                    time_zone_id,
                },
            }
        }

        fn new_naive(date_time: NaiveDateTime) -> BoltDateTime<'static> {
            new(date_time, None, None)
        }

        fn new_unknown_tz(date_time: NaiveDateTime, time_zone_id: &str) -> BoltDateTime {
            new(date_time, Some(0), Some(time_zone_id))
        }

        fn new_tz<Tz: TimeZone>(date_time: DateTime<Tz>, time_zone_id: &str) -> BoltDateTime {
            let local_date_time = date_time.naive_local();
            let utc_offset = date_time.offset().fix().local_minus_utc();
            new(local_date_time, Some(utc_offset.into()), Some(time_zone_id))
        }

        let PackStreamStruct { tag, fields } = s;
        let mut fields = fields.iter();
        let this = match *tag {
            TAG_DATE_TIME_ZONE_ID_V1 if jolt_version == JoltVersion::V1 => {
                let seconds_since_epoch: i64 = next_pack_stream_field(&mut fields)?;
                let nanos: i64 = next_pack_stream_field(&mut fields)?;
                let zone_id: &str = next_pack_stream_field(&mut fields)?;
                if !check_last_pack_stream_field(&mut fields) {
                    return None;
                }
                let (seconds_since_epoch, nanos) =
                    normalize_seconds_nanos(seconds_since_epoch, nanos)?;
                let date_time = DateTime::from_timestamp(seconds_since_epoch, nanos)?.naive_local();
                let Ok(tz) = Tz::from_str(zone_id) else {
                    return Some(new_unknown_tz(date_time, zone_id));
                };
                let date_time = match tz.from_local_datetime(&date_time) {
                    MappedLocalTime::Single(d) => d,
                    MappedLocalTime::Ambiguous(_, _) | MappedLocalTime::None => {
                        return Some(new_unknown_tz(date_time, zone_id))
                    }
                };
                new_tz(date_time, zone_id)
            }
            TAG_DATE_TIME_ZONE_ID_V2 if jolt_version == JoltVersion::V2 => {
                let seconds_since_epoch: i64 = next_pack_stream_field(&mut fields)?;
                let nanos: i64 = next_pack_stream_field(&mut fields)?;
                let zone_id: &str = next_pack_stream_field(&mut fields)?;
                if !check_last_pack_stream_field(&mut fields) {
                    return None;
                }
                let (seconds_since_epoch, nanos) =
                    normalize_seconds_nanos(seconds_since_epoch, nanos)?;
                let utc_date_time =
                    DateTime::from_timestamp(seconds_since_epoch, nanos)?.naive_local();
                let Ok(tz) = Tz::from_str(zone_id) else {
                    return Some(new_unknown_tz(utc_date_time, zone_id));
                };
                new_tz(tz.from_utc_datetime(&utc_date_time), zone_id)
            }
            TAG_DATE_TIME_V1 if jolt_version == JoltVersion::V1 => {
                let seconds_since_epoch: i64 = next_pack_stream_field(&mut fields)?;
                let nanos: i64 = next_pack_stream_field(&mut fields)?;
                let utc_offset_seconds: i64 = next_pack_stream_field(&mut fields)?;
                if !check_last_pack_stream_field(&mut fields) {
                    return None;
                }
                let (seconds_since_epoch, nanos) =
                    normalize_seconds_nanos(seconds_since_epoch, nanos)?;
                let date_time = DateTime::from_timestamp(seconds_since_epoch, nanos)?.naive_local();
                new(date_time, Some(utc_offset_seconds), None)
            }
            TAG_DATE_TIME_V2 if jolt_version == JoltVersion::V2 => {
                let seconds_since_epoch: i64 = next_pack_stream_field(&mut fields)?;
                let nanos: i64 = next_pack_stream_field(&mut fields)?;
                let utc_offset_seconds: i64 = next_pack_stream_field(&mut fields)?;
                if !check_last_pack_stream_field(&mut fields) {
                    return None;
                }
                let (seconds_since_epoch, nanos) =
                    normalize_seconds_nanos(seconds_since_epoch, nanos)?;
                let utc_date_time =
                    DateTime::from_timestamp(seconds_since_epoch, nanos)?.naive_local();
                let utc_date_time = utc_date_time.checked_sub_signed(
                    TimeDelta::new(seconds_since_epoch, nanos).expect("input is normalized"),
                )?;
                new(utc_date_time, Some(utc_offset_seconds), None)
            }
            TAG_LOCAL_DATE_TIME => {
                let seconds_since_epoch: i64 = next_pack_stream_field(&mut fields)?;
                let nanos: i64 = next_pack_stream_field(&mut fields)?;
                if !check_last_pack_stream_field(&mut fields) {
                    return None;
                }
                let (seconds_since_epoch, nanos) =
                    normalize_seconds_nanos(seconds_since_epoch, nanos)?;
                let date_time = UNIX_EPOCH_DATE_TIME + Duration::new(seconds_since_epoch, nanos)?;
                new_naive(date_time.naive_local())
            }
            _ => return None,
        };
        if !check_last_pack_stream_field(&mut fields) {
            return None;
        }
        Some(this)
    }
}

impl BoltDateTime<'_> {
    pub(super) fn jolt_fmt(&self, _jolt_version: JoltVersion) -> impl Display + '_ {
        struct JoltFormatter<'a> {
            this: &'a BoltDateTime<'a>,
        }

        impl Display for JoltFormatter<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.write_str(r#"{"T": ""#)?;
                self.this.date.repr_inner(f)?;
                f.write_str("T")?;
                self.this.time.jolt_fmt_innder(f)?;
                f.write_str(r#""}"#)
            }
        }

        JoltFormatter { this: self }
    }
}
