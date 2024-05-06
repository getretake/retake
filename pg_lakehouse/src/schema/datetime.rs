use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike};
use pgrx::*;
use std::fmt::Debug;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::str::FromStr;
use thiserror::Error;

const NANOSECONDS_IN_SECOND: u32 = 1_000_000_000;

#[derive(Clone, Debug)]
pub struct Date(pub NaiveDate);

#[derive(Clone, Debug)]
pub struct DateTimeNoTz(pub NaiveDateTime);

#[derive(Clone, Debug)]
pub struct Time(pub NaiveTime);

#[derive(Copy, Clone, Debug)]
pub struct PgTypeMod(pub i32);

#[derive(Clone, Debug)]
pub struct DateTimeTz<Tz: TimeZone> {
    datetime: DateTime<Tz>,
    tz: String,
}

impl<Tz: TimeZone> DateTimeTz<Tz> {
    pub fn new(datetime: DateTime<Tz>, tz: &str) -> Self {
        Self {
            datetime,
            tz: tz.to_string(),
        }
    }

    pub fn datetime(&self) -> DateTime<Tz> {
        self.datetime.clone()
    }

    pub fn tz(&self) -> String {
        self.tz.clone()
    }
}

#[derive(Copy, Clone)]
pub enum PgTimestampPrecision {
    Default = -1,
    Second = 0,
    Millisecond = 3,
    Microsecond = 6,
}

impl TryFrom<PgTypeMod> for PgTimestampPrecision {
    type Error = DatetimeError;

    fn try_from(typemod: PgTypeMod) -> Result<Self, Self::Error> {
        let PgTypeMod(typemod) = typemod;

        match typemod {
            -1 => Ok(PgTimestampPrecision::Default),
            1 => Ok(PgTimestampPrecision::Second),
            3 => Ok(PgTimestampPrecision::Millisecond),
            6 => Ok(PgTimestampPrecision::Microsecond),
            unsupported => Err(DatetimeError::UnsupportedTimestampPrecision(unsupported)),
        }
    }
}

impl TryFrom<DateTimeNoTz> for datum::Timestamp {
    type Error = datum::datetime_support::DateTimeConversionError;

    fn try_from(datetime: DateTimeNoTz) -> Result<Self, Self::Error> {
        let DateTimeNoTz(datetime) = datetime;

        datum::Timestamp::new(
            datetime.year(),
            datetime.month() as u8,
            datetime.day() as u8,
            datetime.hour() as u8,
            datetime.minute() as u8,
            (datetime.second() + datetime.nanosecond() / NANOSECONDS_IN_SECOND).into(),
        )
    }
}

impl TryFrom<DateTimeNoTz> for datum::TimestampWithTimeZone {
    type Error = datum::datetime_support::DateTimeConversionError;

    fn try_from(datetime: DateTimeNoTz) -> Result<Self, Self::Error> {
        let DateTimeNoTz(datetime) = datetime;

        datum::TimestampWithTimeZone::new(
            datetime.year(),
            datetime.month() as u8,
            datetime.day() as u8,
            datetime.hour() as u8,
            datetime.minute() as u8,
            (datetime.second() + datetime.nanosecond() / NANOSECONDS_IN_SECOND).into(),
        )
    }
}

impl<Tz> TryFrom<DateTimeTz<Tz>> for datum::TimestampWithTimeZone
where
    Tz: TimeZone + FromStr + RefUnwindSafe + UnwindSafe + Debug,
{
    type Error = datum::datetime_support::DateTimeConversionError;

    fn try_from(datetimetz: DateTimeTz<Tz>) -> Result<Self, Self::Error> {
        let datetime = datetimetz.datetime();
        let tz = datetimetz.tz();

        datum::TimestampWithTimeZone::with_timezone(
            datetime.year(),
            datetime.month() as u8,
            datetime.day() as u8,
            datetime.hour() as u8,
            datetime.minute() as u8,
            (datetime.second() + datetime.nanosecond() / NANOSECONDS_IN_SECOND).into(),
            tz,
        )
    }
}

impl TryFrom<Date> for datum::Date {
    type Error = datum::datetime_support::DateTimeConversionError;

    fn try_from(date: Date) -> Result<Self, Self::Error> {
        let Date(date) = date;
        datum::Date::new(date.year(), date.month() as u8, date.day() as u8)
    }
}

impl TryFrom<Time> for datum::Time {
    type Error = datum::datetime_support::DateTimeConversionError;

    fn try_from(time: Time) -> Result<Self, Self::Error> {
        let Time(time) = time;

        datum::Time::new(
            time.hour() as u8,
            time.minute() as u8,
            time.second() as f64 + time.nanosecond() as f64 / NANOSECONDS_IN_SECOND as f64,
        )
    }
}

#[derive(Error, Debug)]
pub enum DatetimeError {
    #[error("Precision ({0}) is not supported for TIMESTAMP. Supported precisions are 1 for second, 3 for millisecond, and 6 for microsecond")]
    UnsupportedTimestampPrecision(i32),
}
