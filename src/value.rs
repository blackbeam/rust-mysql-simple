use std::str::FromStr;
use std::str::from_utf8;
use std::borrow::ToOwned;
use std::collections::HashMap;
use std::collections::hash_map::Entry::Occupied;
use std::hash::BuildHasherDefault as BldHshrDflt;
use std::io;
use std::io::Write as stdWrite;
use std::time::Duration;
use time::{
    Timespec,
    Tm,
    at,
    now,
    self,
    strptime,
};

use super::consts;
use super::conn::{Column};
use super::error::{
    Error,
    Result as MyResult,
};
use super::error::Error::DriverError;
use super::error::DriverError::MissingNamedParameter;
use super::io::{Write, Read};
use super::conn::Row;

use regex::Regex;

use chrono::{
    NaiveDate,
    NaiveTime,
    NaiveDateTime,
    Datelike,
    Timelike,
};

use byteorder::LittleEndian as LE;
use byteorder::{WriteBytesExt};

use fnv::FnvHasher;

lazy_static! {
    static ref TM_UTCOFF: i32 = now().tm_utcoff;
    static ref TM_ISDST: i32 = now().tm_isdst;
    static ref TIME_RE_HHH_MM_SS: Regex = {
        Regex::new(r"^([0-8]\d\d):([0-5]\d):([0-5]\d)$").unwrap()
    };
    static ref TIME_RE_HHH_MM_SS_MS: Regex = {
        Regex::new(r"^([0-8]\d\d):([0-5]\d):([0-5]\d)\.(\d{1,6})$").unwrap()
    };
    static ref TIME_RE_HH_MM_SS: Regex = {
        Regex::new(r"^(\d{2}):([0-5]\d):([0-5]\d)$").unwrap()
    };
    static ref TIME_RE_HH_MM_SS_MS: Regex = {
        Regex::new(r"^(\d{2}):([0-5]\d):([0-5]\d)\.(\d{1,6})$").unwrap()
    };
    static ref DATETIME_RE_YMD: Regex = {
        Regex::new(r"^(\d{4})-(\d{2})-(\d{2})$").unwrap()
    };
    static ref DATETIME_RE_YMD_HMS: Regex = {
        Regex::new(r"^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$").unwrap()
    };
    static ref DATETIME_RE_YMD_HMS_NS: Regex = {
        Regex::new(r"^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{1,6})$").unwrap()
    };
}

/// `Value` enumerates possible values in mysql cells. Also `Value` used to fill
/// prepared statements.
///
/// Note that to receive something different than `Value::NULL` or `Value::Bytes` from mysql
/// you should use prepared statements.
///
/// If you want to get something more useful from `Value` you should implement
/// [`FromValue`](trait.FromValue.html) on it. To get `T: FromValue` from
/// nullable value you should rely on `FromValue` implemented on `Option<T>`.
///
/// To convert something to `Value` you should implement `Into<Value>` for it.
///
/// ```rust
/// # use mysql::conn::pool;
/// # use mysql::conn::{Opts, OptsBuilder};
/// use mysql::value::from_row;
/// # use std::default::Default;
/// # fn get_opts() -> Opts {
/// #     let user = "root";
/// #     let addr = "127.0.0.1";
/// #     let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or("password".to_string());
/// #     let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
/// #                                .map(|my_port| my_port.parse().ok().unwrap_or(3307))
/// #                                .unwrap_or(3307);
/// #     let mut builder = OptsBuilder::default();
/// #     builder.user(Some(user))
/// #            .pass(Some(pwd))
/// #            .ip_or_hostname(Some(addr))
/// #            .tcp_port(port);
/// #     builder.into()
/// # }
/// # let opts = get_opts();
/// # let pool = pool::Pool::new(opts).unwrap();
/// let mut conn = pool.get_conn().unwrap();
///
/// let result = conn.prep_exec("SELECT ? * ?", (20i32, 0.8_f32)).unwrap();
/// for row in result {
///     let c = from_row::<f32>(row.unwrap());
///     assert_eq!(c, 16.0_f32);
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug)]
pub enum Value {
    NULL,
    Bytes(Vec<u8>),
    Int(i64),
    UInt(u64),
    Float(f64),
    /// year, month, day, hour, minutes, seconds, micro seconds
    Date(u16, u8, u8, u8, u8, u8, u32),
    /// is negative, days, hours, minutes, seconds, micro seconds
    Time(bool, u32, u8, u8, u8, u32)
}

impl Value {
    /// Get correct string representation of a mysql value
    pub fn into_str(&self) -> String {
        match *self {
            Value::NULL => "NULL".to_owned(),
            Value::Bytes(ref x) => {
                String::from_utf8(x.clone()).ok().map_or_else(|| {
                    let mut s = "0x".to_owned();
                    for c in x.iter() {
                        s.extend(format!("{:02X}", *c).chars());
                    }
                    s
                }, |s: String| {
                    let replaced = s.replace("\x5c", "\x5c\x5c")
                                    .replace("\x00", "\x5c\x00")
                                    .replace("\n", "\x5c\n")
                                    .replace("\r", "\x5c\r")
                                    .replace("'", "\x5c'")
                                    .replace("\"", "\x5c\"")
                                    .replace("\x1a", "\x5c\x1a");
                    format!("'{}'", replaced)
                })
            },
            Value::Int(x) => format!("{}", x),
            Value::UInt(x) => format!("{}", x),
            Value::Float(x) => format!("{}", x),
            Value::Date(y, m, d, 0, 0, 0, 0) => format!("'{:04}-{:02}-{:02}'", y, m, d),
            Value::Date(y, m, d, h, i, s, 0) => format!("'{:04}-{:02}-{:02} {:02}:{:02}:{:02}'", y, m, d, h, i, s),
            Value::Date(y, m, d, h, i, s, u) => format!("'{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}'", y, m, d, h, i, s, u),
            Value::Time(neg, d, h, i, s, 0) => {
                if neg {
                    format!("'-{:03}:{:02}:{:02}'", d * 24 + h as u32, i, s)
                } else {
                    format!("'{:03}:{:02}:{:02}'", d * 24 + h as u32, i, s)
                }
            },
            Value::Time(neg, d, h, i, s, u) => {
                if neg {
                    format!("'-{:03}:{:02}:{:02}.{:06}'",
                            d * 24 + h as u32, i, s, u)
                } else {
                    format!("'{:03}:{:02}:{:02}.{:06}'",
                            d * 24 + h as u32, i, s, u)
                }
            }
        }
    }

    #[doc(hidden)]
    pub fn to_bin(&self) -> io::Result<Vec<u8>> {
        let mut writer = Vec::with_capacity(256);
        match *self {
            Value::NULL => (),
            Value::Bytes(ref x) => {
                writer.write_lenenc_bytes(&x[..])?;
            },
            Value::Int(x) => {
                writer.write_i64::<LE>(x)?;
            },
            Value::UInt(x) => {
                writer.write_u64::<LE>(x)?;
            },
            Value::Float(x) => {
                writer.write_f64::<LE>(x)?;
            },
            Value::Date(0u16, 0u8, 0u8, 0u8, 0u8, 0u8, 0u32) => {
                writer.write_u8(0u8)?;
            },
            Value::Date(y, m, d, 0u8, 0u8, 0u8, 0u32) => {
                writer.write_u8(4u8)?;
                writer.write_u16::<LE>(y)?;
                writer.write_u8(m)?;
                writer.write_u8(d)?;
            },
            Value::Date(y, m, d, h, i, s, 0u32) => {
                writer.write_u8(7u8)?;
                writer.write_u16::<LE>(y)?;
                writer.write_u8(m)?;
                writer.write_u8(d)?;
                writer.write_u8(h)?;
                writer.write_u8(i)?;
                writer.write_u8(s)?;
            },
            Value::Date(y, m, d, h, i, s, u) => {
                writer.write_u8(11u8)?;
                writer.write_u16::<LE>(y)?;
                writer.write_u8(m)?;
                writer.write_u8(d)?;
                writer.write_u8(h)?;
                writer.write_u8(i)?;
                writer.write_u8(s)?;
                writer.write_u32::<LE>(u)?;
            },
            Value::Time(_, 0u32, 0u8, 0u8, 0u8, 0u32) => writer.write_u8(0u8)?,
            Value::Time(neg, d, h, m, s, 0u32) => {
                writer.write_u8(8u8)?;
                writer.write_u8(if neg {1u8} else {0u8})?;
                writer.write_u32::<LE>(d)?;
                writer.write_u8(h)?;
                writer.write_u8(m)?;
                writer.write_u8(s)?;
            },
            Value::Time(neg, d, h, m, s, u) => {
                writer.write_u8(12u8)?;
                writer.write_u8(if neg {1u8} else {0u8})?;
                writer.write_u32::<LE>(d)?;
                writer.write_u8(h)?;
                writer.write_u8(m)?;
                writer.write_u8(s)?;
                writer.write_u32::<LE>(u)?;
            }
        };
        Ok(writer)
    }

    #[doc(hidden)]
    pub fn from_payload(pld: &[u8], columns_count: usize) -> io::Result<Vec<Value>> {
        let mut output = Vec::with_capacity(columns_count);
        let mut reader = &pld[..];
        loop {
            if reader.len() == 0 {
                break;
            } else if reader[0] == 0xfb {
                reader = &reader[1..];
                output.push(Value::NULL);
            } else {
                output.push(Value::Bytes(reader.read_lenenc_bytes()?));
            }
        }
        Ok(output)
    }

    #[doc(hidden)]
    pub fn from_bin_payload(pld: &[u8], columns: &[Column]) -> io::Result<Vec<Value>> {
        let bit_offset = 2; // http://dev.mysql.com/doc/internals/en/null-bitmap.html
        let bitmap_len = (columns.len() + 7 + bit_offset) / 8;
        let mut bitmap = Vec::with_capacity(bitmap_len);
        let mut values = Vec::with_capacity(columns.len());
        for i in 0..bitmap_len {
            bitmap.push(pld[i+1]);
        }
        let mut reader = &pld[1 + bitmap_len..];
        for i in 0..columns.len() {
            let c = &columns[i];
            if bitmap[(i + bit_offset) / 8] & (1 << ((i + bit_offset) % 8)) == 0 {
                values.push(reader.read_bin_value(c.column_type,
                                                  c.flags.contains(consts::UNSIGNED_FLAG))?);
            } else {
                values.push(Value::NULL);
            }
        }
        Ok(values)
    }

    // (NULL-bitmap, values, ids of fields to send throwgh send_long_data)
    #[doc(hidden)]
    pub fn to_bin_payload(params: &[Column], values: &[Value], max_allowed_packet: usize) -> io::Result<(Vec<u8>, Vec<u8>, Option<Vec<u16>>)> {
        let bitmap_len = (params.len() + 7) / 8;
        let mut large_ids = Vec::new();
        let mut writer = Vec::new();
        let mut bitmap = vec![0u8; bitmap_len];
        let mut i = 0u16;
        let mut written = 0;
        let cap = max_allowed_packet - bitmap_len - values.len() * 8;
        for value in values.iter() {
            match *value {
                Value::NULL => bitmap[i as usize / 8] |= 1 << ((i % 8u16) as usize),
                _ => {
                    let val = value.to_bin()?;
                    if val.len() < cap - written {
                        written += val.len();
                        writer.write_all(&val[..])?;
                    } else {
                        large_ids.push(i);
                    }
                }
            }
            i += 1;
        }
        if large_ids.len() == 0 {
            Ok((bitmap, writer, None))
        } else {
            Ok((bitmap, writer, Some(large_ids)))
        }
    }
}

macro_rules! rollback {
    ($x:ident) => (match $x {
        Ok(x) => x.rollback(),
        Err(Error::FromValueError(x)) => x,
        _ => unreachable!(),
    });
}

/// Will *panic* if could not convert `row` to `T`.
#[inline]
pub fn from_row<T: FromRow>(row: Row) -> T {
    FromRow::from_row(row)
}

/// Will return `Err(row)` if could not convert `row` to `T`
#[inline]
pub fn from_row_opt<T: FromRow>(row: Row) -> MyResult<T> {
    FromRow::from_row_opt(row)
}

/// Trait to convert `Row` into tuple of `FromValue` implementors up to arity 12.
pub trait FromRow {
    fn from_row(row: Row) -> Self;
    fn from_row_opt(row: Row) -> MyResult<Self> where Self: Sized;
}

macro_rules! take_or_place {
    ($row:expr, $index:expr, $t:ident) => (
        match $row.take($index) {
            Some(value) => {
                match $t::get_intermediate(value) {
                    Ok(ir) => ir,
                    Err(Error::FromValueError(value)) => {
                        $row.place($index, value);
                        return Err(Error::FromRowError($row));
                    },
                    _ => unreachable!(),
                }
            },
            None => return Err(Error::FromRowError($row)),
        }
    );
    ($row:expr, $index:expr, $t:ident, $( [$idx:expr, $ir:expr] ),*) => (
        match $row.take($index) {
            Some(value) => {
                match $t::get_intermediate(value) {
                    Ok(ir) => ir,
                    Err(Error::FromValueError(value)) => {
                        $($row.place($idx, $ir.rollback());)*
                        $row.place($index, value);
                        return Err(Error::FromRowError($row));
                    },
                    _ => unreachable!(),
                }
            },
            None => return Err(Error::FromRowError($row)),
        }
    );
}

impl<T, Ir> FromRow for T
where Ir: ConvIr<T>,
      T: FromValue<Intermediate=Ir> {
    #[inline]
    fn from_row(row: Row) -> T {
        FromRow::from_row_opt(row).ok().expect("Could not convert row to T")
    }
    fn from_row_opt(mut row: Row) -> MyResult<T> {
        if row.len() == 1 {
            Ok(take_or_place!(row, 0, T).commit())
        } else {
            Err(Error::FromRowError(row))
        }
    }
}

impl<T1, Ir1> FromRow for (T1,)
where Ir1: ConvIr<T1>,
      T1: FromValue<Intermediate=Ir1> {
    #[inline]
    fn from_row(row: Row) -> (T1,) {
        FromRow::from_row_opt(row).ok().expect("Could not convert row to (T1,)")
    }
    fn from_row_opt(row: Row) -> MyResult<(T1,)> {
        T1::from_row_opt(row).map(|t| (t,))
    }
}

impl<T1, Ir1,
     T2, Ir2> FromRow for (T1, T2)
where Ir1: ConvIr<T1>, T1: FromValue<Intermediate=Ir1>,
      Ir2: ConvIr<T2>, T2: FromValue<Intermediate=Ir2> {
    #[inline]
    fn from_row(row: Row) -> (T1, T2) {
        FromRow::from_row_opt(row).ok().expect("Could not convert row to (T1,T2)")
    }
    fn from_row_opt(mut row: Row) -> MyResult<(T1, T2)> {
        if row.len() != 2 {
            return Err(Error::FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        Ok((ir1.commit(), ir2.commit()))
    }
}

impl<T1, Ir1,
     T2, Ir2,
     T3, Ir3,> FromRow for (T1, T2, T3)
where Ir1: ConvIr<T1>, T1: FromValue<Intermediate=Ir1>,
      Ir2: ConvIr<T2>, T2: FromValue<Intermediate=Ir2>,
      Ir3: ConvIr<T3>, T3: FromValue<Intermediate=Ir3>, {
    #[inline]
    fn from_row(row: Row) -> (T1, T2, T3) {
        FromRow::from_row_opt(row).ok().expect("Could not convert row to (T1,T2,T3)")
    }
    fn from_row_opt(mut row: Row) -> MyResult<(T1, T2, T3)> {
        if row.len() != 3 {
            return Err(Error::FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        Ok((
            ir1.commit(),
            ir2.commit(),
            ir3.commit(),
        ))
    }
}

impl<T1, Ir1,
     T2, Ir2,
     T3, Ir3,
     T4, Ir4,> FromRow for (T1, T2, T3, T4)
where Ir1: ConvIr<T1>, T1: FromValue<Intermediate=Ir1>,
      Ir2: ConvIr<T2>, T2: FromValue<Intermediate=Ir2>,
      Ir3: ConvIr<T3>, T3: FromValue<Intermediate=Ir3>,
      Ir4: ConvIr<T4>, T4: FromValue<Intermediate=Ir4>, {
    #[inline]
    fn from_row(row: Row) -> (T1, T2, T3, T4) {
        FromRow::from_row_opt(row).ok().expect("Could not convert row to (T1 .. T4)")
    }
    fn from_row_opt(mut row: Row) -> MyResult<(T1, T2, T3, T4)> {
        if row.len() != 4 {
            return Err(Error::FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(row, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        Ok((
            ir1.commit(),
            ir2.commit(),
            ir3.commit(),
            ir4.commit(),
        ))
    }
}

impl<T1, Ir1,
     T2, Ir2,
     T3, Ir3,
     T4, Ir4,
     T5, Ir5,> FromRow for (T1, T2, T3, T4, T5)
where Ir1: ConvIr<T1>, T1: FromValue<Intermediate=Ir1>,
      Ir2: ConvIr<T2>, T2: FromValue<Intermediate=Ir2>,
      Ir3: ConvIr<T3>, T3: FromValue<Intermediate=Ir3>,
      Ir4: ConvIr<T4>, T4: FromValue<Intermediate=Ir4>,
      Ir5: ConvIr<T5>, T5: FromValue<Intermediate=Ir5>, {
    #[inline]
    fn from_row(row: Row) -> (T1, T2, T3, T4, T5) {
        FromRow::from_row_opt(row).ok().expect("Could not convert row to (T1 .. T5)")
    }
    fn from_row_opt(mut row: Row) -> MyResult<(T1, T2, T3, T4, T5)> {
        if row.len() != 5 {
            return Err(Error::FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(row, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(row, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        Ok((
            ir1.commit(),
            ir2.commit(),
            ir3.commit(),
            ir4.commit(),
            ir5.commit(),
        ))
    }
}

impl<T1, Ir1,
     T2, Ir2,
     T3, Ir3,
     T4, Ir4,
     T5, Ir5,
     T6, Ir6,> FromRow for (T1, T2, T3, T4, T5, T6)
where Ir1: ConvIr<T1>, T1: FromValue<Intermediate=Ir1>,
      Ir2: ConvIr<T2>, T2: FromValue<Intermediate=Ir2>,
      Ir3: ConvIr<T3>, T3: FromValue<Intermediate=Ir3>,
      Ir4: ConvIr<T4>, T4: FromValue<Intermediate=Ir4>,
      Ir5: ConvIr<T5>, T5: FromValue<Intermediate=Ir5>,
      Ir6: ConvIr<T6>, T6: FromValue<Intermediate=Ir6>, {
    #[inline]
    fn from_row(row: Row) -> (T1, T2, T3, T4, T5, T6) {
        FromRow::from_row_opt(row).ok().expect("Could not convert row to (T1 .. T6)")
    }
    fn from_row_opt(mut row: Row) ->
        MyResult<(T1, T2, T3, T4, T5, T6)>
    {
        if row.len() != 6 {
            return Err(Error::FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(row, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(row, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(row, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        Ok((
            ir1.commit(),
            ir2.commit(),
            ir3.commit(),
            ir4.commit(),
            ir5.commit(),
            ir6.commit(),
        ))
    }
}

impl<T1, Ir1,
     T2, Ir2,
     T3, Ir3,
     T4, Ir4,
     T5, Ir5,
     T6, Ir6,
     T7, Ir7,> FromRow for (T1, T2, T3, T4, T5, T6, T7)
where Ir1: ConvIr<T1>, T1: FromValue<Intermediate=Ir1>,
      Ir2: ConvIr<T2>, T2: FromValue<Intermediate=Ir2>,
      Ir3: ConvIr<T3>, T3: FromValue<Intermediate=Ir3>,
      Ir4: ConvIr<T4>, T4: FromValue<Intermediate=Ir4>,
      Ir5: ConvIr<T5>, T5: FromValue<Intermediate=Ir5>,
      Ir6: ConvIr<T6>, T6: FromValue<Intermediate=Ir6>,
      Ir7: ConvIr<T7>, T7: FromValue<Intermediate=Ir7>, {
    #[inline]
    fn from_row(row: Row) -> (T1, T2, T3, T4, T5, T6, T7) {
        FromRow::from_row_opt(row).ok().expect("Could not convert row to (T1 .. T7)")
    }
    fn from_row_opt(mut row: Row) ->
        MyResult<(T1, T2, T3, T4, T5, T6, T7)>
    {
        if row.len() != 7 {
            return Err(Error::FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(row, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(row, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(row, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        let ir7 = take_or_place!(row, 6, T7,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]);
        Ok((
            ir1.commit(),
            ir2.commit(),
            ir3.commit(),
            ir4.commit(),
            ir5.commit(),
            ir6.commit(),
            ir7.commit(),
        ))
    }
}

impl<T1, Ir1,
     T2, Ir2,
     T3, Ir3,
     T4, Ir4,
     T5, Ir5,
     T6, Ir6,
     T7, Ir7,
     T8, Ir8,> FromRow for (T1, T2, T3, T4, T5, T6, T7, T8)
where Ir1: ConvIr<T1>, T1: FromValue<Intermediate=Ir1>,
      Ir2: ConvIr<T2>, T2: FromValue<Intermediate=Ir2>,
      Ir3: ConvIr<T3>, T3: FromValue<Intermediate=Ir3>,
      Ir4: ConvIr<T4>, T4: FromValue<Intermediate=Ir4>,
      Ir5: ConvIr<T5>, T5: FromValue<Intermediate=Ir5>,
      Ir6: ConvIr<T6>, T6: FromValue<Intermediate=Ir6>,
      Ir7: ConvIr<T7>, T7: FromValue<Intermediate=Ir7>,
      Ir8: ConvIr<T8>, T8: FromValue<Intermediate=Ir8>, {
    #[inline]
    fn from_row(row: Row) -> (T1, T2, T3, T4, T5, T6, T7, T8) {
        FromRow::from_row_opt(row).ok().expect("Could not convert row to (T1 .. T8)")
    }
    fn from_row_opt(mut row: Row) ->
        MyResult<(T1, T2, T3, T4, T5, T6, T7, T8)>
    {
        if row.len() != 8 {
            return Err(Error::FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(row, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(row, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(row, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        let ir7 = take_or_place!(
            row, 6, T7,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]
        );
        let ir8 = take_or_place!(
            row, 7, T8,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7]
        );
        Ok((
            ir1.commit(),
            ir2.commit(),
            ir3.commit(),
            ir4.commit(),
            ir5.commit(),
            ir6.commit(),
            ir7.commit(),
            ir8.commit(),
        ))
    }
}

impl<T1, Ir1,
     T2, Ir2,
     T3, Ir3,
     T4, Ir4,
     T5, Ir5,
     T6, Ir6,
     T7, Ir7,
     T8, Ir8,
     T9, Ir9,> FromRow for (T1, T2, T3, T4, T5, T6, T7, T8, T9)
where Ir1: ConvIr<T1>, T1: FromValue<Intermediate=Ir1>,
      Ir2: ConvIr<T2>, T2: FromValue<Intermediate=Ir2>,
      Ir3: ConvIr<T3>, T3: FromValue<Intermediate=Ir3>,
      Ir4: ConvIr<T4>, T4: FromValue<Intermediate=Ir4>,
      Ir5: ConvIr<T5>, T5: FromValue<Intermediate=Ir5>,
      Ir6: ConvIr<T6>, T6: FromValue<Intermediate=Ir6>,
      Ir7: ConvIr<T7>, T7: FromValue<Intermediate=Ir7>,
      Ir8: ConvIr<T8>, T8: FromValue<Intermediate=Ir8>,
      Ir9: ConvIr<T9>, T9: FromValue<Intermediate=Ir9>, {
    #[inline]
    fn from_row(row: Row) -> (T1, T2, T3, T4, T5, T6, T7, T8, T9) {
        FromRow::from_row_opt(row).ok().expect("Could not convert row to (T1 .. T9)")
    }
    fn from_row_opt(mut row: Row) ->
        MyResult<(T1, T2, T3, T4, T5, T6, T7, T8, T9)>
    {
        if row.len() != 9 {
            return Err(Error::FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(row, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(row, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(row, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        let ir7 = take_or_place!(
            row, 6, T7,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]
        );
        let ir8 = take_or_place!(
            row, 7, T8,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7]
        );
        let ir9 = take_or_place!(
            row, 8, T9,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7],
            [7, ir8]
        );
        Ok((
            ir1.commit(),
            ir2.commit(),
            ir3.commit(),
            ir4.commit(),
            ir5.commit(),
            ir6.commit(),
            ir7.commit(),
            ir8.commit(),
            ir9.commit(),
        ))
    }
}

impl<T1, Ir1,
     T2, Ir2,
     T3, Ir3,
     T4, Ir4,
     T5, Ir5,
     T6, Ir6,
     T7, Ir7,
     T8, Ir8,
     T9, Ir9,
     T10, Ir10,> FromRow for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)
where Ir1: ConvIr<T1>, T1: FromValue<Intermediate=Ir1>,
      Ir2: ConvIr<T2>, T2: FromValue<Intermediate=Ir2>,
      Ir3: ConvIr<T3>, T3: FromValue<Intermediate=Ir3>,
      Ir4: ConvIr<T4>, T4: FromValue<Intermediate=Ir4>,
      Ir5: ConvIr<T5>, T5: FromValue<Intermediate=Ir5>,
      Ir6: ConvIr<T6>, T6: FromValue<Intermediate=Ir6>,
      Ir7: ConvIr<T7>, T7: FromValue<Intermediate=Ir7>,
      Ir8: ConvIr<T8>, T8: FromValue<Intermediate=Ir8>,
      Ir9: ConvIr<T9>, T9: FromValue<Intermediate=Ir9>,
      Ir10: ConvIr<T10>, T10: FromValue<Intermediate=Ir10>, {
    #[inline]
    fn from_row(row: Row) -> (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10) {
        FromRow::from_row_opt(row).ok().expect("Could not convert row to (T1 .. T10)")
    }
    fn from_row_opt(mut row: Row) ->
        MyResult<(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)>
    {
        if row.len() != 10 {
            return Err(Error::FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(row, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(row, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(row, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        let ir7 = take_or_place!(
            row, 6, T7,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]
        );
        let ir8 = take_or_place!(
            row, 7, T8,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7]
        );
        let ir9 = take_or_place!(
            row, 8, T9,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7],
            [7, ir8]
        );
        let ir10 = take_or_place!(
            row, 9, T10,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7],
            [7, ir8], [8, ir9]
        );
        Ok((
            ir1.commit(),
            ir2.commit(),
            ir3.commit(),
            ir4.commit(),
            ir5.commit(),
            ir6.commit(),
            ir7.commit(),
            ir8.commit(),
            ir9.commit(),
            ir10.commit(),
        ))
    }
}

impl<T1, Ir1,
     T2, Ir2,
     T3, Ir3,
     T4, Ir4,
     T5, Ir5,
     T6, Ir6,
     T7, Ir7,
     T8, Ir8,
     T9, Ir9,
     T10, Ir10,
     T11, Ir11,> FromRow for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)
where Ir1: ConvIr<T1>, T1: FromValue<Intermediate=Ir1>,
      Ir2: ConvIr<T2>, T2: FromValue<Intermediate=Ir2>,
      Ir3: ConvIr<T3>, T3: FromValue<Intermediate=Ir3>,
      Ir4: ConvIr<T4>, T4: FromValue<Intermediate=Ir4>,
      Ir5: ConvIr<T5>, T5: FromValue<Intermediate=Ir5>,
      Ir6: ConvIr<T6>, T6: FromValue<Intermediate=Ir6>,
      Ir7: ConvIr<T7>, T7: FromValue<Intermediate=Ir7>,
      Ir8: ConvIr<T8>, T8: FromValue<Intermediate=Ir8>,
      Ir9: ConvIr<T9>, T9: FromValue<Intermediate=Ir9>,
      Ir10: ConvIr<T10>, T10: FromValue<Intermediate=Ir10>,
      Ir11: ConvIr<T11>, T11: FromValue<Intermediate=Ir11>, {
    #[inline]
    fn from_row(row: Row) -> (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11) {
        FromRow::from_row_opt(row).ok().expect("Could not convert row to (T1 .. T11)")
    }
    fn from_row_opt(mut row: Row) ->
        MyResult<(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)>
    {
        if row.len() != 11 {
            return Err(Error::FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(row, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(row, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(row, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        let ir7 = take_or_place!(
            row, 6, T7,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]
        );
        let ir8 = take_or_place!(
            row, 7, T8,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7]
        );
        let ir9 = take_or_place!(
            row, 8, T9,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7],
            [7, ir8]
        );
        let ir10 = take_or_place!(
            row, 9, T10,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7],
            [7, ir8], [8, ir9]
        );
        let ir11 = take_or_place!(
            row, 10, T11,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7],
            [7, ir8], [8, ir9], [9, ir10]
        );
        Ok((
            ir1.commit(),
            ir2.commit(),
            ir3.commit(),
            ir4.commit(),
            ir5.commit(),
            ir6.commit(),
            ir7.commit(),
            ir8.commit(),
            ir9.commit(),
            ir10.commit(),
            ir11.commit(),
        ))
    }
}

impl<T1, Ir1,
     T2, Ir2,
     T3, Ir3,
     T4, Ir4,
     T5, Ir5,
     T6, Ir6,
     T7, Ir7,
     T8, Ir8,
     T9, Ir9,
     T10, Ir10,
     T11, Ir11,
     T12, Ir12,> FromRow for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)
where Ir1: ConvIr<T1>, T1: FromValue<Intermediate=Ir1>,
      Ir2: ConvIr<T2>, T2: FromValue<Intermediate=Ir2>,
      Ir3: ConvIr<T3>, T3: FromValue<Intermediate=Ir3>,
      Ir4: ConvIr<T4>, T4: FromValue<Intermediate=Ir4>,
      Ir5: ConvIr<T5>, T5: FromValue<Intermediate=Ir5>,
      Ir6: ConvIr<T6>, T6: FromValue<Intermediate=Ir6>,
      Ir7: ConvIr<T7>, T7: FromValue<Intermediate=Ir7>,
      Ir8: ConvIr<T8>, T8: FromValue<Intermediate=Ir8>,
      Ir9: ConvIr<T9>, T9: FromValue<Intermediate=Ir9>,
      Ir10: ConvIr<T10>, T10: FromValue<Intermediate=Ir10>,
      Ir11: ConvIr<T11>, T11: FromValue<Intermediate=Ir11>,
      Ir12: ConvIr<T12>, T12: FromValue<Intermediate=Ir12>, {
    #[inline]
    fn from_row(row: Row) -> (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12) {
        FromRow::from_row_opt(row).ok().expect("Could not convert row to (T1 .. T12)")
    }
    fn from_row_opt(mut row: Row) ->
        MyResult<(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)>
    {
        if row.len() != 12 {
            return Err(Error::FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(row, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(row, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(row, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        let ir7 = take_or_place!(
            row, 6, T7,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]
        );
        let ir8 = take_or_place!(
            row, 7, T8,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7]
        );
        let ir9 = take_or_place!(
            row, 8, T9,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7],
            [7, ir8]
        );
        let ir10 = take_or_place!(
            row, 9, T10,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7],
            [7, ir8], [8, ir9]
        );
        let ir11 = take_or_place!(
            row, 10, T11,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7],
            [7, ir8], [8, ir9], [9, ir10]
        );
        let ir12 = take_or_place!(
            row, 11, T12,
            [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7],
            [7, ir8], [8, ir9], [9, ir10], [10, ir11]
        );
        Ok((
            ir1.commit(),
            ir2.commit(),
            ir3.commit(),
            ir4.commit(),
            ir5.commit(),
            ir6.commit(),
            ir7.commit(),
            ir8.commit(),
            ir9.commit(),
            ir10.commit(),
            ir11.commit(),
            ir12.commit(),
        ))
    }
}

#[derive(PartialEq, Clone, Debug)]
pub enum Params {
    Empty,
    Named(HashMap<String, Value, BldHshrDflt<FnvHasher>>),
    Positional(Vec<Value>),
}

impl Params {
    pub fn into_positional(self, named_params: &Vec<String>) -> MyResult<Params> {
        match self {
            Params::Named(mut map) => {
                let mut params: Vec<Value> = Vec::with_capacity(named_params.len());
                'params: for (i, name) in named_params.clone().into_iter().enumerate() {
                    if ! map.contains_key(&name) {
                        return Err(DriverError(MissingNamedParameter(name.clone())));
                    }
                    match map.entry(name.clone()) {
                        Occupied(entry) => {
                            let mut x = named_params.len() - 1;
                            while x > i {
                                if name == named_params[x] {
                                    params.push(entry.get().clone());
                                    continue 'params;
                                }
                                x -= 1;
                            }
                            params.push(entry.remove());
                        },
                        _ => unreachable!(),
                    }
                }
                Ok(Params::Positional(params))
            },
            params => Ok(params),
        }
    }
}

#[macro_export]
macro_rules! params {
    ($($name:expr => $value:expr),*) => (
        vec![
            $((::std::string::String::from($name), $crate::Value::from($value))),*
        ]
    );
    ($($name:expr => $value:expr),*,) => (
        params!($($name => $value),*)
    );
}

impl<'a, T: Into<Params> + Clone> From<&'a T> for Params {
    fn from(x: &'a T) -> Params {
        x.clone().into()
    }
}

impl<T: Into<Value>> From<Vec<T>> for Params {
    fn from(x: Vec<T>) -> Params {
        let mut raw_params = Vec::with_capacity(x.len());
        for v in x.into_iter() {
            raw_params.push(v.into());
        }
        if raw_params.len() == 0 {
            Params::Empty
        } else {
            Params::Positional(raw_params)
        }
    }
}

impl<N: Into<String>, V: Into<Value>> From<Vec<(N, V)>> for Params {
    fn from(x: Vec<(N, V)>) -> Params {
        let mut map = HashMap::default();
        for (name, value) in x.into_iter() {
            let name = name.into();
            if map.contains_key(&name) {
                panic!("Redefinition of named parameter `{}'", name);
            } else {
                map.insert(name, value.into());
            }
        }
        Params::Named(map)
    }
}

impl<'a> From<&'a [&'a ToValue]> for Params {
    fn from(x: &'a [&'a ToValue]) -> Params {
        let mut raw_params = Vec::with_capacity(x.len());
        for v in x.into_iter() {
            raw_params.push(v.to_value());
        }
        if raw_params.len() == 0 {
            Params::Empty
        } else {
            Params::Positional(raw_params)
        }
    }
}

impl From<()> for Params {
    fn from(_: ()) -> Params {
        Params::Empty
    }
}

macro_rules! into_params_impl {
    ($([$A:ident,$a:ident]),*) => (
        impl<$($A: Into<Value>,)*> From<($($A,)*)> for Params {
            fn from(x: ($($A,)*)) -> Params {
                let ($($a,)*) = x;
                Params::Positional(vec![$($a.into(),)*])
            }
        }
    );
}

into_params_impl!([A,a]);
into_params_impl!([A,a],[B,b]);
into_params_impl!([A,a],[B,b],[C,c]);
into_params_impl!([A,a],[B,b],[C,c],[D,d]);
into_params_impl!([A,a],[B,b],[C,c],[D,d],[E,e]);
into_params_impl!([A,a],[B,b],[C,c],[D,d],[E,e],[F,f]);
into_params_impl!([A,a],[B,b],[C,c],[D,d],[E,e],[F,f],[G,g]);
into_params_impl!([A,a],[B,b],[C,c],[D,d],[E,e],[F,f],[G,g],[H,h]);
into_params_impl!([A,a],[B,b],[C,c],[D,d],[E,e],[F,f],[G,g],[H,h],[I,i]);
into_params_impl!([A,a],[B,b],[C,c],[D,d],[E,e],[F,f],[G,g],[H,h],[I,i],[J,j]);
into_params_impl!([A,a],[B,b],[C,c],[D,d],[E,e],[F,f],[G,g],[H,h],[I,i],[J,j],[K,k]);
into_params_impl!([A,a],[B,b],[C,c],[D,d],[E,e],[F,f],[G,g],[H,h],[I,i],[J,j],[K,k],[L,l]);

pub trait ToValue {
    fn to_value(&self) -> Value;
}

impl<T: Into<Value> + Clone> ToValue for T {
    fn to_value(&self) -> Value {
        self.clone().into()
    }
}

impl<'a, T: ToValue> From<&'a T> for Value {
    fn from(x: &'a T) -> Value {
        x.to_value()
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(x: Option<T>) -> Value {
        match x {
            None => Value::NULL,
            Some(x) => x.into(),
        }
    }
}

macro_rules! into_value_impl (
    (i64) => (
        impl From<i64> for Value {
            fn from(x: i64) -> Value {
                Value::Int(x)
            }
        }
    );
    ($t:ty) => (
        impl From<$t> for Value {
            fn from(x: $t) -> Value {
                Value::Int(x as i64)
            }
        }
    );
);

into_value_impl!(i8);
into_value_impl!(u8);
into_value_impl!(i16);
into_value_impl!(u16);
into_value_impl!(i32);
into_value_impl!(u32);
into_value_impl!(isize);
into_value_impl!(i64);

impl From<u64> for Value {
    fn from(x: u64) -> Value {
        Value::UInt(x)
    }
}

impl From<usize> for Value {
    fn from(x: usize) -> Value {
        if x as u64 <= ::std::i64::MAX as u64 {
            Value::Int(x as i64)
        } else {
            Value::UInt(x as u64)
        }
    }
}

impl From<f32> for Value {
    fn from(x: f32) -> Value {
        Value::Float(x as f64)
    }
}

impl From<f64> for Value {
    fn from(x: f64) -> Value {
        Value::Float(x)
    }
}

impl From<bool> for Value {
    fn from(x: bool) -> Value {
        Value::Int(if x {1} else {0})
    }
}

impl<'a> From<&'a [u8]> for Value {
    fn from(x: &'a [u8]) -> Value {
        Value::Bytes(x.into())
    }
}

impl From<Vec<u8>> for Value {
    fn from(x: Vec<u8>) -> Value {
        Value::Bytes(x)
    }
}

impl<'a> From<&'a str> for Value {
    fn from(x: &'a str) -> Value {
        let string: String = x.into();
        Value::Bytes(string.into_bytes())
    }
}

impl From<String> for Value {
    fn from(x: String) -> Value {
        Value::Bytes(x.into_bytes())
    }
}



impl From<NaiveDateTime> for Value {
    fn from(x: NaiveDateTime) -> Value {
        if 1000 > x.year() || x.year() > 9999 {
            panic!("Year `{}` not in supported range [1000, 9999]", x.year())
        }
        Value::Date(
            x.year() as u16,
            x.month() as u8,
            x.day() as u8,
            x.hour() as u8,
            x.minute() as u8,
            x.second() as u8,
            x.nanosecond() / 1000,
        )
    }
}

impl From<NaiveDate> for Value {
    fn from(x: NaiveDate) -> Value {
        if 1000 > x.year() || x.year() > 9999 {
            panic!("Year `{}` not in supported range [1000, 9999]", x.year())
        }
        Value::Date(
            x.year() as u16,
            x.month() as u8,
            x.day() as u8,
            0,
            0,
            0,
            0
        )
    }
}

impl From<NaiveTime> for Value {
    fn from(x: NaiveTime) -> Value {
        Value::Time(
            false,
            0,
            x.hour() as u8,
            x.minute() as u8,
            x.second() as u8,
            x.nanosecond() / 1000,
        )
    }
}

impl From<Timespec> for Value {
    fn from(x: Timespec) -> Value {
        let t = at(x);
        Value::Date(
             t.tm_year as u16 + 1_900,
             (t.tm_mon + 1) as u8,
             t.tm_mday as u8,
             t.tm_hour as u8,
             t.tm_min as u8,
             t.tm_sec as u8,
             t.tm_nsec as u32 / 1000)
    }
}

impl From<Duration> for Value {
    fn from(x: Duration) -> Value {
        let mut secs_total = x.as_secs();
        let micros = (x.subsec_nanos() as f64 / 1000_f64).round() as u32;
        let seconds = (secs_total % 60) as u8;
        secs_total -= seconds as u64;
        let minutes = ((secs_total % (60 * 60)) / 60) as u8;
        secs_total -= (minutes as u64) * 60;
        let hours = ((secs_total % (60 * 60 * 24)) / (60 * 60)) as u8;
        secs_total -= (hours as u64) * 60 * 60;
        Value::Time(false, (secs_total / (60 * 60 * 24)) as u32, hours, minutes, seconds, micros)
    }
}

impl From<time::Duration> for Value {
    fn from(mut x: time::Duration) -> Value {
        let negative = x < time::Duration::zero();
        if negative {
            x = -x;
        }
        let days = x.num_days() as u32;
        x = x - time::Duration::days(x.num_days());
        let hours = x.num_hours() as u8;
        x = x - time::Duration::hours(x.num_hours());
        let minutes = x.num_minutes() as u8;
        x = x - time::Duration::minutes(x.num_minutes());
        let seconds = x.num_seconds() as u8;
        x = x - time::Duration::seconds(x.num_seconds());
        let microseconds = x.num_microseconds().unwrap_or(0) as u32;
        Value::Time(negative, days, hours, minutes, seconds, microseconds)
    }
}

macro_rules! from_array_impl {
    ($n:expr) => {
        impl From<[u8; $n]> for Value {
            fn from(x: [u8; $n]) -> Value {
                Value::from(&x[..])
            }
        }
    };
}

from_array_impl!(0);
from_array_impl!(1);
from_array_impl!(2);
from_array_impl!(3);
from_array_impl!(4);
from_array_impl!(5);
from_array_impl!(6);
from_array_impl!(7);
from_array_impl!(8);
from_array_impl!(9);
from_array_impl!(10);
from_array_impl!(11);
from_array_impl!(12);
from_array_impl!(13);
from_array_impl!(14);
from_array_impl!(15);
from_array_impl!(16);
from_array_impl!(17);
from_array_impl!(18);
from_array_impl!(19);
from_array_impl!(20);
from_array_impl!(21);
from_array_impl!(22);
from_array_impl!(23);
from_array_impl!(24);
from_array_impl!(25);
from_array_impl!(26);
from_array_impl!(27);
from_array_impl!(28);
from_array_impl!(29);
from_array_impl!(30);
from_array_impl!(31);
from_array_impl!(32);

/// Basic operations on `FromValue` conversion intermediate result.
///
/// See [`FromValue`](trait.FromValue.html)
pub trait ConvIr<T>: Sized {
    fn new(v: Value) -> MyResult<Self>;
    fn commit(self) -> T;
    fn rollback(self) -> Value;
}

/// Implement this trait to convert value to something.
///
/// `FromRow` requires ability to cheaply rollback `FromValue` conversion. This ability is
/// provided via `Intermediate` associated type.
///
/// Example implementation:
///
/// ```ignore
/// #[derive(Debug)]
/// pub struct StringIr {
///     bytes: Vec<u8>,
/// }
///
/// impl ConvIr<String> for StringIr {
///     fn new(v: Value) -> MyResult<StringIr> {
///         match v {
///             Value::Bytes(bytes) => match from_utf8(&*bytes) {
///                 Ok(_) => Ok(StringIr { bytes: bytes }),
///                 Err(_) => Err(Error::FromValueError(Value::Bytes(bytes))),
///             },
///             v => Err(Error::FromValueError(v)),
///         }
///     }
///     fn commit(self) -> String {
///         unsafe { String::from_utf8_unchecked(self.bytes) }
///     }
///     fn rollback(self) -> Value {
///         Value::Bytes(self.bytes)
///     }
/// }
///
/// impl FromValue for String {
///     type Intermediate = StringIr;
/// }
/// ```
pub trait FromValue: Sized {
    type Intermediate: ConvIr<Self>;

    /// Will panic if could not convert `v` to `Self`.
    fn from_value(v: Value) -> Self {
        Self::from_value_opt(v).ok().expect("Could not retrieve Self from Value")
    }

    /// Will return `Err(Error::FromValueError(v))` if could not convert `v` to `Self`.
    fn from_value_opt(v: Value) -> MyResult<Self> {
        let ir = Self::Intermediate::new(v)?;
        Ok(ir.commit())
    }

    /// Will return `Err(Error::FromValueError(v))` if `v` is not convertible to `Self`.
    fn get_intermediate(v: Value) -> MyResult<Self::Intermediate> {
        Self::Intermediate::new(v)
    }
}

macro_rules! impl_from_value {
    ($ty:ty, $ir:ty, $msg:expr) => (
        impl FromValue for $ty {
            type Intermediate = $ir;
            fn from_value(v: Value) -> $ty {
                <Self as FromValue>::from_value_opt(v).ok().expect($msg)
            }
        }
    );
}



#[derive(Debug)]
pub struct ParseIr<T> {
    value: Value,
    output: T,
}

#[derive(Debug)]
pub struct BytesIr {
    bytes: Vec<u8>,
}

#[derive(Debug)]
pub struct StringIr {
    bytes: Vec<u8>,
}

macro_rules! impl_from_value_num_2 {
    ($t:ident, $msg:expr) => (
        impl ConvIr<$t> for ParseIr<$t> {
            fn new(v: Value) -> MyResult<ParseIr<$t>> {
                match v {
                    Value::Int(x) => {
                        let min = ::std::$t::MIN as i64;
                        let mut max = ::std::$t::MAX as i64;
                        if max < 0 {
                            max = ::std::i64::MAX;
                        }
                        if min <= x && x <= max {
                            Ok(ParseIr {
                                value: Value::Int(x),
                                output: x as $t,
                            })
                        } else {
                            Err(Error::FromValueError(Value::Int(x)))
                        }
                    },
                    Value::UInt(x) if x <= ::std::$t::MAX as u64 => Ok(ParseIr {
                        value: Value::UInt(x),
                        output: x as $t,
                    }),
                    Value::Bytes(bytes) => {
                        let val = from_utf8(&*bytes).ok().and_then(|x| {
                            FromStr::from_str(x).ok()
                        });
                        match val {
                            Some(x) => Ok(ParseIr {
                                value: Value::Bytes(bytes),
                                output: x,
                            }),
                            None => Err(Error::FromValueError(Value::Bytes(bytes))),
                        }
                    },
                    v => Err(Error::FromValueError(v)),
                }
            }
            fn commit(self) -> $t {
                self.output
            }
            fn rollback(self) -> Value {
                self.value
            }
        }

        impl_from_value!($t, ParseIr<$t>, $msg);
    );
}

#[derive(Debug)]
pub struct OptionIr<T> {
    value: Option<Value>,
    ir: Option<T>,
}

impl<T, Ir> ConvIr<Option<T>> for OptionIr<Ir>
where T: FromValue<Intermediate=Ir>,
      Ir: ConvIr<T> {
    fn new(v: Value) -> MyResult<OptionIr<Ir>> {
        match v {
            Value::NULL => Ok(OptionIr {
                value: Some(Value::NULL),
                ir: None,
            }),
            v => {
                match T::get_intermediate(v) {
                    Ok(ir) => Ok(OptionIr {
                        value: None,
                        ir: Some(ir),
                    }),
                    Err(Error::FromValueError(v)) => {
                        Err(Error::FromValueError(v))
                    },
                    _ => unreachable!(),
                }
            }
        }
    }
    fn commit(self) -> Option<T> {
        match self.ir {
            Some(ir) => Some(ir.commit()),
            None => None,
        }
    }
    fn rollback(self) -> Value {
        let OptionIr { value, ir } = self;
        match value {
            Some(v) => v,
            None => match ir {
                Some(ir) => ir.rollback(),
                None => unreachable!(),
            }
        }
    }
}

impl<T> FromValue for Option<T>
where T: FromValue {
    type Intermediate = OptionIr<T::Intermediate>;
    fn from_value(v: Value) -> Option<T> {
        <Self as FromValue>::from_value_opt(v).ok()
            .expect("Could not retrieve Option<T> from Value")
    }
}

#[derive(Debug)]
pub struct ValueIr {
    value: Value,
}

impl ConvIr<Value> for ValueIr {
    fn new(v: Value) -> MyResult<ValueIr> {
        Ok(ValueIr { value: v })
    }
    fn commit(self) -> Value {
        self.value
    }
    fn rollback(self) -> Value {
        self.value
    }
}

impl FromValue for Value {
    type Intermediate = ValueIr;
    fn from_value(v: Value) -> Value {
        v
    }
    fn from_value_opt(v: Value) -> MyResult<Value> {
        Ok(v)
    }
}

impl ConvIr<String> for StringIr {
    fn new(v: Value) -> MyResult<StringIr> {
        match v {
            Value::Bytes(bytes) => match from_utf8(&*bytes) {
                Ok(_) => Ok(StringIr { bytes: bytes }),
                Err(_) => Err(Error::FromValueError(Value::Bytes(bytes))),
            },
            v => Err(Error::FromValueError(v)),
        }
    }
    fn commit(self) -> String {
        unsafe { String::from_utf8_unchecked(self.bytes) }
    }
    fn rollback(self) -> Value {
        Value::Bytes(self.bytes)
    }
}

impl ConvIr<i64> for ParseIr<i64> {
    fn new(v: Value) -> MyResult<ParseIr<i64>> {
        match v {
            Value::Int(x) => Ok(ParseIr {
                value: Value::Int(x),
                output: x,
            }),
            Value::UInt(x) if x <= ::std::i64::MAX as u64 => Ok(ParseIr {
                value: Value::UInt(x),
                output: x as i64,
            }),
            Value::Bytes(bytes) => {
                let val = from_utf8(&*bytes).ok().and_then(|x| i64::from_str(x).ok());
                match val {
                    Some(x) => Ok(ParseIr {
                        value: Value::Bytes(bytes),
                        output: x,
                    }),
                    None => Err(Error::FromValueError(Value::Bytes(bytes))),
                }
            },
            v => Err(Error::FromValueError(v)),
        }
    }
    fn commit(self) -> i64 {
        self.output
    }
    fn rollback(self) -> Value {
        self.value
    }
}

impl ConvIr<u64> for ParseIr<u64> {
    fn new(v: Value) -> MyResult<ParseIr<u64>> {
        match v {
            Value::Int(x) if x >= 0 => Ok(ParseIr {
                value: Value::Int(x),
                output: x as u64,
            }),
            Value::UInt(x) => Ok(ParseIr {
                value: Value::UInt(x),
                output: x,
            }),
            Value::Bytes(bytes) => {
                let val = from_utf8(&*bytes).ok().and_then(|x| u64::from_str(x).ok());
                match val {
                    Some(x) => Ok(ParseIr {
                        value: Value::Bytes(bytes),
                        output: x,
                    }),
                    _ => Err(Error::FromValueError(Value::Bytes(bytes))),
                }
            },
            v => Err(Error::FromValueError(v)),
        }
    }
    fn commit(self) -> u64 {
        self.output
    }
    fn rollback(self) -> Value {
        self.value
    }
}

impl ConvIr<f32> for ParseIr<f32> {
    fn new(v: Value) -> MyResult<ParseIr<f32>> {
        match v {
            Value::Float(x) if x >= ::std::f32::MIN as f64 && x <= ::std::f32::MAX as f64 => {
                Ok(ParseIr {
                    value: Value::Float(x),
                    output: x as f32,
                })
            },
            Value::Bytes(bytes) => {
                let val = from_utf8(&*bytes).ok().and_then(|x| f32::from_str(x).ok());
                match val {
                    Some(x) => Ok(ParseIr {
                        value: Value::Bytes(bytes),
                        output: x,
                    }),
                    None => Err(Error::FromValueError(Value::Bytes(bytes))),
                }
            },
            v => Err(Error::FromValueError(v)),
        }
    }
    fn commit(self) -> f32 {
        self.output
    }
    fn rollback(self) -> Value {
        self.value
    }
}

impl ConvIr<f64> for ParseIr<f64> {
    fn new(v: Value) -> MyResult<ParseIr<f64>> {
        match v {
            Value::Float(x) => Ok(ParseIr {
                value: Value::Float(x),
                output: x,
            }),
            Value::Bytes(bytes) => {
                let val = from_utf8(&*bytes).ok().and_then(|x| f64::from_str(x).ok());
                match val {
                    Some(x) => Ok(ParseIr {
                        value: Value::Bytes(bytes),
                        output: x,
                    }),
                    _ => Err(Error::FromValueError(Value::Bytes(bytes))),
                }
            },
            v => Err(Error::FromValueError(v)),
        }
    }
    fn commit(self) -> f64 {
        self.output
    }
    fn rollback(self) -> Value {
        self.value
    }
}

impl ConvIr<bool> for ParseIr<bool> {
    fn new(v: Value) -> MyResult<ParseIr<bool>> {
        match v {
            Value::Int(0) => Ok(ParseIr {
                value: Value::Int(0),
                output: false,
            }),
            Value::Int(1) => Ok(ParseIr {
                value: Value::Int(1),
                output: true,
            }),
            Value::Bytes(bytes) => {
                if bytes.len() == 1 {
                    match bytes[0] {
                        0x30 => Ok(ParseIr {
                            value: Value::Bytes(bytes),
                            output: false,
                        }),
                        0x31 => Ok(ParseIr {
                            value: Value::Bytes(bytes),
                            output: true,
                        }),
                        _ => Err(Error::FromValueError(Value::Bytes(bytes))),
                    }
                } else {
                    Err(Error::FromValueError(Value::Bytes(bytes)))
                }
            },
            v => Err(Error::FromValueError(v)),
        }
    }
    fn commit(self) -> bool {
        self.output
    }
    fn rollback(self) -> Value {
        self.value
    }
}

impl ConvIr<Vec<u8>> for BytesIr {
    fn new(v: Value) -> MyResult<BytesIr> {
        match v {
            Value::Bytes(bytes) => Ok(BytesIr {
                bytes: bytes,
            }),
            v => Err(Error::FromValueError(v)),
        }
    }
    fn commit(self) -> Vec<u8> {
        self.bytes
    }
    fn rollback(self) -> Value {
        Value::Bytes(self.bytes)
    }
}

impl ConvIr<Timespec> for ParseIr<Timespec> {
    fn new (v: Value) -> MyResult<ParseIr<Timespec>> {
        match v {
            Value::Date(y, m, d, h, i, s, u) => Ok(ParseIr {
                value: Value::Date(y, m, d, h, i, s, u),
                output: Tm {
                    tm_year: y as i32 - 1_900,
                    tm_mon: m as i32 - 1,
                    tm_mday: d as i32,
                    tm_hour: h as i32,
                    tm_min: i as i32,
                    tm_sec: s as i32,
                    tm_nsec: u as i32 * 1_000,
                    tm_utcoff: *TM_UTCOFF,
                    tm_wday: 0,
                    tm_yday: 0,
                    tm_isdst: *TM_ISDST,
                }.to_timespec()
            }),
            Value::Bytes(bytes) => {
                let val = from_utf8(&*bytes).ok().and_then(|s| {
                    strptime(s, "%Y-%m-%d %H:%M:%S").or(strptime(s, "%Y-%m-%d")).ok()
                }).map(|mut tm| {
                    tm.tm_utcoff = *TM_UTCOFF;
                    tm.tm_isdst = *TM_ISDST;
                    tm.to_timespec()
                });
                match val {
                    Some(timespec) => Ok(ParseIr {
                        value: Value::Bytes(bytes),
                        output: timespec,
                    }),
                    None => Err(Error::FromValueError(Value::Bytes(bytes))),
                }
            },
            v => Err(Error::FromValueError(v)),
        }
    }
    fn commit(self) -> Timespec {
        self.output
    }
    fn rollback(self) -> Value {
        self.value
    }
}

impl ConvIr<NaiveDateTime> for ParseIr<NaiveDateTime> {
    fn new (v: Value) -> MyResult<ParseIr<NaiveDateTime>> {
        let result = match v {
            Value::Date(y, m, d, h, i, s, u) => {
                let date = NaiveDate::from_ymd_opt(y as i32, m as u32, d as u32);
                let time = NaiveTime::from_hms_micro_opt(h as u32, i as u32, s as u32, u);
                Ok((date, time, Value::Date(y, m, d, h, i, s, u)))
            },
            Value::Bytes(bytes) => {
                if let Some((y, m, d, h, i, s, u)) = parse_mysql_datetime_string(&*bytes) {
                    let date = NaiveDate::from_ymd_opt(y as i32, m, d);
                    let time = NaiveTime::from_hms_micro_opt(h, i, s, u);
                    Ok((date, time, Value::Bytes(bytes)))
                } else {
                    Err(Error::FromValueError(Value::Bytes(bytes)))
                }
            },
            v => Err(Error::FromValueError(v)),
        };

        let (date, time, value) = result?;

        if date.is_some() && time.is_some() {
            Ok(ParseIr {
                value: value,
                output: NaiveDateTime::new(date.unwrap(), time.unwrap()),
            })
        } else {
            Err(Error::FromValueError(value))
        }
    }
    fn commit(self) -> NaiveDateTime {
        self.output
    }
    fn rollback(self) -> Value {
        self.value
    }
}

impl ConvIr<NaiveDate> for ParseIr<NaiveDate> {
    fn new (v: Value) -> MyResult<ParseIr<NaiveDate>> {
        let result = match v {
            Value::Date(y, m, d, h, i, s, u) => {
                let date = NaiveDate::from_ymd_opt(y as i32, m as u32, d as u32);
                Ok((date, Value::Date(y, m, d, h, i, s, u)))
            },
            Value::Bytes(bytes) => {
                if let Some((y, m, d, _, _, _, _)) = parse_mysql_datetime_string(&*bytes) {
                    let date = NaiveDate::from_ymd_opt(y as i32, m, d);
                    Ok((date, Value::Bytes(bytes)))
                } else {
                    Err(Error::FromValueError(Value::Bytes(bytes)))
                }
            },
            v => Err(Error::FromValueError(v)),
        };

        let (date, value) = result?;

        if date.is_some() {
            Ok(ParseIr {
                value: value,
                output: date.unwrap(),
            })
        } else {
            Err(Error::FromValueError(value))
        }
    }
    fn commit(self) -> NaiveDate {
        self.output
    }
    fn rollback(self) -> Value {
        self.value
    }
}

/// Returns (year, month, day, hour, minute, second, micros)
fn parse_mysql_datetime_string(bytes: &[u8]) -> Option<(u32, u32, u32, u32, u32, u32, u32)> {
    if bytes.len() == 0 {
        return None;
    }
    from_utf8(&*bytes).ok().and_then(|s| {
        let s_ref = s.as_ref();
        DATETIME_RE_YMD_HMS.captures(s_ref)
        .or_else(|| {
            DATETIME_RE_YMD.captures(s_ref)
        }).or_else(|| {
            DATETIME_RE_YMD_HMS_NS.captures(s_ref)
        })
    }).map(|capts| {
        let year = capts.get(1).unwrap().as_str().parse::<u32>().unwrap();
        let month = capts.get(2).unwrap().as_str().parse::<u32>().unwrap();
        let day = capts.get(3).unwrap().as_str().parse::<u32>().unwrap();
        let (hour, minute, second, micros) = if capts.len() > 4 {
            let hour = capts.get(4).unwrap().as_str().parse::<u32>().unwrap();
            let minute = capts.get(5).unwrap().as_str().parse::<u32>().unwrap();
            let second = capts.get(6).unwrap().as_str().parse::<u32>().unwrap();
            let micros = if capts.len() == 8 {
                let micros_str = capts.get(7).unwrap();
                let mut left_zero_cnt = 0;
                for b in micros_str.as_str().bytes() {
                    if b == b'0' {
                        left_zero_cnt += 1;
                    } else {
                        break;
                    }
                }
                let mut micros = micros_str.as_str().parse::<u32>().unwrap();
                for _ in 0..(6 - left_zero_cnt - (micros_str.as_str().len() - left_zero_cnt)) {
                    micros *= 10;
                }
                micros
            } else {
                0
            };
            (hour, minute, second, micros)
        } else {
            (0, 0, 0, 0)
        };
        (
            year,
            month,
            day,
            hour,
            minute,
            second,
            micros,
        )
    })
}

/// Returns (is_neg, hours, minutes, seconds, microseconds)
fn parse_mysql_time_string(mut bytes: &[u8]) -> Option<(bool, u32, u32, u32, u32)> {
    if bytes.len() == 0 {
        return None;
    }
    let is_neg = bytes[0] == b'-';
    if is_neg {
        bytes = &bytes[1..];
    }
    from_utf8(bytes).ok().and_then(|time_str| {
        let t_ref = time_str.as_ref();
        TIME_RE_HHH_MM_SS.captures(t_ref)
        .or_else(|| {
            TIME_RE_HHH_MM_SS_MS.captures(t_ref)
        }).or_else(|| {
            TIME_RE_HH_MM_SS.captures(t_ref)
        }).or_else(|| {
            TIME_RE_HH_MM_SS_MS.captures(t_ref)
        })
    }).map(|capts| {
        let hours = capts.get(1).unwrap().as_str().parse::<u32>().unwrap();
        let minutes = capts.get(2).unwrap().as_str().parse::<u32>().unwrap();
        let seconds = capts.get(3).unwrap().as_str().parse::<u32>().unwrap();
        let microseconds = if capts.len() == 5 {
            let micros_str = capts.get(4).unwrap();
            let mut left_zero_cnt = 0;
            for b in micros_str.as_str().bytes() {
                if b == b'0' {
                    left_zero_cnt += 1;
                } else {
                    break;
                }
            }
            let mut micros = capts.get(4).unwrap().as_str().parse::<u32>().unwrap();
            for _ in 0..(6 - left_zero_cnt - (micros_str.as_str().len() - left_zero_cnt)) {
                micros *= 10;
            }
            micros
        } else {
            0
        };
        (is_neg, hours, minutes, seconds, microseconds)
    })
}

impl ConvIr<NaiveTime> for ParseIr<NaiveTime> {
    fn new(v: Value) -> MyResult<ParseIr<NaiveTime>> {
        let result = match v {
            Value::Time(false, 0, h, m, s, u) => {
                let time = NaiveTime::from_hms_micro_opt(h as u32, m as u32, s as u32, u);
                Ok((time, Value::Time(false, 0, h, m, s, u)))
            },
            Value::Bytes(bytes) => {
                if let Some((false, h, m, s, u)) = parse_mysql_time_string(&*bytes) {
                    let time = NaiveTime::from_hms_micro_opt(h, m, s, u);
                    Ok((time, Value::Bytes(bytes)))
                } else {
                    Err(Error::FromValueError(Value::Bytes(bytes)))
                }
            },
            v => Err(Error::FromValueError(v))
        };

        let (time, value) = result?;

        if time.is_some() {
            Ok(ParseIr {
                value: value,
                output: time.unwrap(),
            })
        } else {
            Err(Error::FromValueError(value))
        }
    }
    fn commit(self) -> NaiveTime {
        self.output
    }
    fn rollback(self) -> Value {
        self.value
    }
}

impl ConvIr<Duration> for ParseIr<Duration> {
    fn new(v: Value) -> MyResult<ParseIr<Duration>> {
        match v {
            Value::Time(false, days, hours, minutes, seconds, microseconds) => {
                let nanos = (microseconds as u32) * 1000;
                let secs = seconds as u64
                         + minutes as u64 * 60
                         + hours as u64 * 60 * 60
                         + days as u64 * 60 * 60 * 24;
                Ok(ParseIr {
                    value: Value::Time(false, days, hours, minutes, seconds, microseconds),
                    output: Duration::new(secs, nanos),
                })
            },
            Value::Bytes(val_bytes) => {
                let duration = match parse_mysql_time_string(&*val_bytes) {
                    Some((false, hours, minutes, seconds, microseconds)) => {
                        let nanos = microseconds * 1000;
                        let secs = seconds as u64
                                 + minutes as u64 * 60
                                 + hours as u64 * 60 * 60;
                        Duration::new(secs, nanos)
                    },
                    _ => return Err(Error::FromValueError(Value::Bytes(val_bytes))),
                };
                Ok(ParseIr {
                    value: Value::Bytes(val_bytes),
                    output: duration,
                })
            },
            v => Err(Error::FromValueError(v)),
        }
    }
    fn commit(self) -> Duration {
        self.output
    }
    fn rollback(self) -> Value {
        self.value
    }
}

impl ConvIr<time::Duration> for ParseIr<time::Duration> {
    fn new(v: Value) -> MyResult<ParseIr<time::Duration>> {
        match v {
            Value::Time(is_neg, days, hours, minutes, seconds, microseconds) => {
                let duration = time::Duration::days(days as i64)
                             + time::Duration::hours(hours as i64)
                             + time::Duration::minutes(minutes as i64)
                             + time::Duration::seconds(seconds as i64)
                             + time::Duration::microseconds(microseconds as i64);
                Ok(ParseIr {
                    value: Value::Time(is_neg, days, hours, minutes, seconds, microseconds),
                    output: if is_neg { -duration } else { duration }
                })
            },
            Value::Bytes(val_bytes) => {
                let duration = match parse_mysql_time_string(&*val_bytes) {
                    Some((is_neg, hours, minutes, seconds, microseconds)) => {
                        let duration = time::Duration::hours(hours as i64)
                                     + time::Duration::minutes(minutes as i64)
                                     + time::Duration::seconds(seconds as i64)
                                     + time::Duration::microseconds(microseconds as i64);
                        if is_neg { -duration } else { duration }
                    },
                    _ => return Err(Error::FromValueError(Value::Bytes(val_bytes))),
                };
                Ok(ParseIr {
                    value: Value::Bytes(val_bytes),
                    output: duration,
                })
            },
            v => Err(Error::FromValueError(v)),
        }
    }
    fn commit(self) -> time::Duration {
        self.output
    }
    fn rollback(self) -> Value {
        self.value
    }
}

impl_from_value!(NaiveDateTime, ParseIr<NaiveDateTime>,
                 "Could not retrieve NaiveDateTime from Value");
impl_from_value!(NaiveDate, ParseIr<NaiveDate>, "Could not retrieve NaiveDate from Value");
impl_from_value!(NaiveTime, ParseIr<NaiveTime>, "Could not retrieve NaiveTime from Value");
impl_from_value!(Timespec, ParseIr<Timespec>, "Could not retrieve Timespec from Value");
impl_from_value!(Duration, ParseIr<Duration>, "Could not retrieve Duration from Value");
impl_from_value!(time::Duration, ParseIr<time::Duration>,
                 "Could not retrieve time::Duration from Value");
impl_from_value!(String, StringIr, "Could not retrieve String from Value");
impl_from_value!(Vec<u8>, BytesIr, "Could not retrieve Vec<u8> from Value");
impl_from_value!(bool, ParseIr<bool>, "Could not retrieve bool from Value");
impl_from_value!(i64, ParseIr<i64>, "Could not retrieve i64 from Value");
impl_from_value!(u64, ParseIr<u64>, "Could not retrieve u64 from Value");
impl_from_value!(f32, ParseIr<f32>, "Could not retrieve f32 from Value");
impl_from_value!(f64, ParseIr<f64>, "Could not retrieve f64 from Value");
impl_from_value_num_2!(i8, "Could not retrieve i8 from Value");
impl_from_value_num_2!(u8, "Could not retrieve u8 from Value");
impl_from_value_num_2!(i16, "Could not retrieve i16 from Value");
impl_from_value_num_2!(u16, "Could not retrieve u16 from Value");
impl_from_value_num_2!(i32, "Could not retrieve i32 from Value");
impl_from_value_num_2!(u32, "Could not retrieve u32 from Value");
impl_from_value_num_2!(isize, "Could not retrieve isize from Value");
impl_from_value_num_2!(usize, "Could not retrieve usize from Value");

/// Will panic if could not convert `v` to `T`
#[inline]
pub fn from_value<T: FromValue>(v: Value) -> T {
    FromValue::from_value(v)
}

/// Will return `Err(v)` if could not convert `v` to `T`
#[inline]
pub fn from_value_opt<T: FromValue>(v: Value) -> MyResult<T> {
    FromValue::from_value_opt(v)
}

#[cfg(test)]
#[allow(non_snake_case)]
mod test {
    mod into_str {
        use super::super::Value::{Bytes, Int, UInt, Date, Time, Float, NULL};
        #[test]
        fn should_convert_NULL_to_mysql_string() {
            assert_eq!(NULL.into_str(), "NULL".to_string());
        }
        #[test]
        fn should_convert_Bytes_to_mysql_string() {
            assert_eq!(Bytes(b"hello".to_vec()).into_str(),
                       "'hello'".to_string());
        }
        #[test]
        fn should_escape_specials_while_converting_Bytes() {
            assert_eq!(Bytes(b"h\x5c'e'l'l'o".to_vec()).into_str(),
                       "'h\x5c\x5c\x5c'e\x5c'l\x5c'l\x5c'o'".to_string());
        }
        #[test]
        fn should_use_hex_literals_for_binary_Bytes() {
            assert_eq!(Bytes(b"\x00\x01\x02\x03\x04\xFF".to_vec()).into_str(),
                       "0x0001020304FF".to_string());
        }
        #[test]
        fn should_convert_Int_to_mysql_string() {
            assert_eq!(Int(-65536).into_str(), "-65536".to_string());
        }
        #[test]
        fn should_convert_UInt_to_mysql_string() {
            assert_eq!(UInt(4294967296).into_str(), "4294967296".to_string());
        }
        #[test]
        fn should_convert_Float_to_mysql_string() {
            assert_eq!(Float(686.868).into_str(), "686.868".to_string());
        }
        #[test]
        fn should_convert_Date_to_mysql_string() {
            assert_eq!(Date(0, 0, 0, 0, 0, 0, 0).into_str(),
                       "'0000-00-00'".to_string());
            assert_eq!(Date(2014, 2, 20, 0, 0, 0, 0).into_str(),
                       "'2014-02-20'".to_string());
            assert_eq!(Date(2014, 2, 20, 22, 20, 10, 0).into_str(),
                       "'2014-02-20 22:20:10'".to_string());
            assert_eq!(Date(2014, 2, 20, 22, 20, 10, 1).into_str(),
                       "'2014-02-20 22:20:10.000001'".to_string())
        }
        #[test]
        fn should_convert_Time_to_mysql_string() {
            assert_eq!(Time(false, 0, 0, 0, 0, 0).into_str(),
                       "'000:00:00'".to_string());
            assert_eq!(Time(true, 34, 3, 2, 1, 0).into_str(),
                       "'-819:02:01'".to_string());
            assert_eq!(Time(false, 10, 100, 20, 30, 40).into_str(),
                       "'340:20:30.000040'".to_string());
        }
    }

    mod from_value {
        use super::super::from_value;
        use super::super::Value::{Bytes, Date, Int, Time};
        use time::{Timespec, now, self};
        use Conn;
        use Opts;
        use OptsBuilder;
        use chrono::{
            NaiveDate,
            NaiveTime,
            NaiveDateTime,
        };

        static USER: &'static str = "root";
        static PASS: &'static str = "password";
        static ADDR: &'static str = "127.0.0.1";
        static PORT: u16          = 3307;

        #[cfg(all(feature = "ssl", target_os = "macos"))]
        pub fn get_opts() -> Opts {
            let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or(PASS.to_string());
            let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
                .map(|my_port| my_port.parse().ok().unwrap_or(PORT))
                .unwrap_or(PORT);
            let mut builder = OptsBuilder::default();
            builder.user(Some(USER))
                .pass(Some(pwd))
                .ip_or_hostname(Some(ADDR))
                .tcp_port(port)
                .init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'"])
                .ssl_opts(Some(Some(("tests/client.p12", "pass", vec!["tests/ca-cert.cer"]))));
            builder.into()
        }

        #[cfg(all(feature = "ssl", all(unix, not(target_os = "macos"))))]
        pub fn get_opts() -> Opts {
            let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or(PASS.to_string());
            let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
                                       .map(|my_port| my_port.parse().ok().unwrap_or(PORT))
                                       .unwrap_or(PORT);
            let mut builder = OptsBuilder::default();
            builder.user(Some(USER))
                   .pass(Some(pwd))
                   .ip_or_hostname(Some(ADDR))
                   .tcp_port(port)
                   .init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'"])
                   .ssl_opts(Some(("tests/ca-cert.pem", None::<(String, String)>)));
            builder.into()
        }

        #[cfg(any(not(feature = "ssl"), target_os = "windows"))]
        pub fn get_opts() -> Opts {
            let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or(PASS.to_string());
            let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
                                       .map(|my_port| my_port.parse().ok().unwrap_or(PORT))
                                       .unwrap_or(PORT);
            let mut builder = OptsBuilder::default();
            builder.user(Some(USER))
                   .pass(Some(pwd))
                   .ip_or_hostname(Some(ADDR))
                   .tcp_port(port)
                   .init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'"]);
            builder.into()
        }

        #[test]
        fn should_convert_Bytes_to_Timespec() {
            assert_eq!(
                Timespec { sec: 1414866780 - now().tm_utcoff as i64,nsec: 0 },
                from_value::<Timespec>(Bytes(b"2014-11-01 18:33:00".to_vec()))
            );
            assert_eq!(
                Timespec {
                    sec: 1414866780 - now().tm_utcoff as i64,
                    nsec: 1000,
                },
                from_value::<Timespec>(Date(2014, 11, 1, 18, 33, 00, 1)));
            assert_eq!(
                Timespec { sec: 1414800000 - now().tm_utcoff as i64, nsec: 0 },
                from_value::<Timespec>(Bytes(b"2014-11-01".to_vec())));
        }

        #[test]
        fn should_convert_Bytes_to_NaiveDateTime() {
            assert_eq!(
                NaiveDateTime::new(
                    NaiveDate::from_ymd(2014, 11, 1),
                    NaiveTime::from_hms_micro(18, 33, 00, 1),
                ),
                from_value::<NaiveDateTime>(Bytes(b"2014-11-01 18:33:00.000001".to_vec()))
            );
            assert_eq!(
                NaiveDateTime::new(
                    NaiveDate::from_ymd(2014, 11, 1),
                    NaiveTime::from_hms(18, 33, 00),
                ),
                from_value::<NaiveDateTime>(Bytes(b"2014-11-01 18:33:00".to_vec()))
            );
            assert_eq!(
                NaiveDateTime::new(
                    NaiveDate::from_ymd(2014, 11, 1),
                    NaiveTime::from_hms(0, 0, 0),
                ),
                from_value::<NaiveDateTime>(Bytes(b"2014-11-01".to_vec()))
            );
        }

        #[test]
        fn should_convert_Bytes_to_NaiveDate() {
            assert_eq!(
                NaiveDate::from_ymd(2014, 11, 1),
                from_value::<NaiveDate>(Bytes(b"2014-11-01".to_vec()))
            );
        }

        #[test]
        fn should_convert_Bytes_to_NaiveTime() {
            assert_eq!(
                NaiveTime::from_hms(23, 58, 57),
                from_value::<NaiveTime>(Bytes(b"23:58:57".to_vec()))
            );
            assert_eq!(
                NaiveTime::from_hms_micro(23, 58, 57, 20),
                from_value::<NaiveTime>(Bytes(b"23:58:57.00002".to_vec()))
            );
        }

        #[test]
        fn should_convert_Value_to_NaiveDateTime() {
            assert_eq!(
                NaiveDateTime::new(
                    NaiveDate::from_ymd(2014, 11, 1),
                    NaiveTime::from_hms_micro(18, 33, 0, 1),
                ),
                from_value::<NaiveDateTime>(Date(2014, 11, 1, 18, 33, 00, 1))
            );
        }

        #[test]
        fn should_convert_Value_to_NaiveDate() {
            assert_eq!(
                NaiveDate::from_ymd(2014, 11, 1),
                from_value::<NaiveDate>(Date(2014, 11, 1, 0, 0, 0, 0))
            );
        }

        #[test]
        fn should_convert_Value_to_NaiveTime() {
            assert_eq!(
                NaiveTime::from_hms_micro(18, 33, 0, 20),
                from_value::<NaiveTime>(Time(false, 0, 18, 33, 0, 20))
            );
        }

        #[test]
        fn stored_and_retrieved_timestamp_should_match() {
            let mut conn = Conn::new(get_opts()).unwrap();
            conn.query("CREATE TEMPORARY TABLE x.t (ts TIMESTAMP)").unwrap();
            let ts = Timespec { sec: 1414866780, nsec: 0 };
            conn.prep_exec("INSERT INTO x.t (ts) VALUES (?)", (ts,)).unwrap();
            let mut x = conn.prep_exec("SELECT * FROM x.t", ()).unwrap().next().unwrap().unwrap();
            assert_eq!(ts, from_value::<Timespec>(x.take(0).unwrap()));
            conn.prep_exec("DELETE FROM x.t", ()).unwrap();
            let ts = NaiveDateTime::new(
                NaiveDate::from_ymd(2014, 11, 1),
                NaiveTime::from_hms(18, 33, 0),
            );
            conn.prep_exec("INSERT INTO x.t (ts) VALUES (?)", (ts,)).unwrap();
            let mut x = conn.prep_exec("SELECT * FROM x.t", ()).unwrap().next().unwrap().unwrap();
            assert_eq!(ts, from_value::<NaiveDateTime>(x.take(0).unwrap()));
            conn.prep_exec("DELETE FROM x.t", ()).unwrap();
            let ts = NaiveDate::from_ymd(2014, 11, 1);
            conn.prep_exec("INSERT INTO x.t (ts) VALUES (?)", (ts,)).unwrap();
            let mut x = conn.prep_exec("SELECT * FROM x.t", ()).unwrap().next().unwrap().unwrap();
            assert_eq!(ts, from_value::<NaiveDate>(x.take(0).unwrap()));
        }

        #[test]
        fn stored_and_retrieved_datetime_should_match() {
            let mut conn = Conn::new(get_opts()).unwrap();
            conn.query("CREATE TEMPORARY TABLE x.t (ts DATETIME)").unwrap();
            let ts = Timespec { sec: 1414866780, nsec: 0 };
            conn.prep_exec("INSERT INTO x.t (ts) VALUES (?)", (ts,)).unwrap();
            let mut x = conn.prep_exec("SELECT * FROM x.t", ()).unwrap().next().unwrap().unwrap();
            assert_eq!(ts, from_value::<Timespec>(x.take(0).unwrap()));
            conn.prep_exec("DELETE FROM x.t", ()).unwrap();
            let ts = NaiveDateTime::new(
                NaiveDate::from_ymd(2014, 11, 1),
                NaiveTime::from_hms(18, 33, 0),
            );
            conn.prep_exec("INSERT INTO x.t (ts) VALUES (?)", (ts,)).unwrap();
            let mut x = conn.prep_exec("SELECT * FROM x.t", ()).unwrap().next().unwrap().unwrap();
            assert_eq!(ts, from_value::<NaiveDateTime>(x.take(0).unwrap()));
            conn.prep_exec("DELETE FROM x.t", ()).unwrap();
            let ts = NaiveDate::from_ymd(2014, 11, 1);
            conn.prep_exec("INSERT INTO x.t (ts) VALUES (?)", (ts,)).unwrap();
            let mut x = conn.prep_exec("SELECT * FROM x.t", ()).unwrap().next().unwrap().unwrap();
            assert_eq!(ts, from_value::<NaiveDate>(x.take(0).unwrap()));
        }

        #[test]
        fn should_convert_Bytes_to_time_Duration() {
            assert_eq!(-time::Duration::milliseconds(433830500),
                       from_value(Bytes(b"-120:30:30.5".to_vec())));
            assert_eq!(-time::Duration::milliseconds(433830500),
                       from_value(Bytes(b"-120:30:30.500000".to_vec())));
            assert_eq!(-time::Duration::milliseconds(433830005),
                       from_value(Bytes(b"-120:30:30.005".to_vec())));
        }

        #[test]
        fn should_convert_signed_to_unsigned() {
            assert_eq!(1, from_value::<usize>(Int(1)));
        }

        #[test]
        #[should_panic]
        fn should_not_convert_negative_to_unsigned() {
            from_value::<u64>(Int(-1));
        }
    }

    mod to_value {
        use super::super::Value;
        use super::super::Value::{Date, Time};
        use time::{Timespec, now, self};

        #[test]
        fn should_convert_Time_to_Date() {
            let ts = Timespec {
                sec: 1414866780 - now().tm_utcoff as i64,
                nsec: 1000,
            };
            assert_eq!(Value::from(ts), Date(2014, 11, 1, 18, 33, 00, 1));
        }
        #[test]
        fn should_convert_time_Duration_to_Time() {
            let pos_dur = time::Duration::milliseconds(433830500);
            let neg_dur = -pos_dur;
            assert_eq!(Value::from(pos_dur), Time(false, 5, 0, 30, 30, 500000));
            assert_eq!(Value::from(neg_dur), Time(true, 5, 0, 30, 30, 500000));
        }
    }

    mod from_row {
        use Column;
        use std::iter::repeat;
        use std::sync::Arc;
        use time::{Timespec, now};
        use super::super::{from_row, from_row_opt, Value};
        use super::super::super::error::Error;
        use super::super::super::conn::Row;

        #[test]
        fn should_convert_to_tuples() {
            let col = Column::from_payload(b"\x03def\x06schema\x05table\x09org_table\x04name\x08\
                                           org_name\x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\
                                           \x00\x00".to_vec()).unwrap();
            let t1 = Value::Int(1);
            let t2 = Value::Bytes(b"a".to_vec());
            let t3 = Value::Bytes(vec![255]);
            let t4 = Value::Date(2014, 11, 1, 18, 33, 00, 1);
            let o1 = 1u8;
            let o2 = "a".to_string();
            let o3 = vec![255];
            let o4 = Timespec { sec: 1414866780 - now().tm_utcoff as i64, nsec: 1000 };
            let v1 = vec![t1.clone()];
            let v2 = vec![t1.clone(), t2.clone()];
            let v3 = vec![t1.clone(), t2.clone(), t3.clone()];
            let v4 = vec![t1.clone(), t2.clone(), t3.clone(), t4.clone()];
            let v5 = vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                          t1.clone()];
            let v6 = vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                          t1.clone(), t2.clone()];
            let v7 = vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                          t1.clone(), t2.clone(), t3.clone()];
            let v8 = vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                          t1.clone(), t2.clone(), t3.clone(), t4.clone()];
            let v9 = vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                          t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                          t1.clone()];
            let v10 = vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                           t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                           t1.clone(), t2.clone()];
            let v11 = vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                           t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                           t1.clone(), t2.clone(), t3.clone()];
            let v12 = vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                           t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                           t1.clone(), t2.clone(), t3.clone(), t4.clone()];
            let r1 = (o1,);
            let r2 = (o1, o2.clone());
            let r3 = (o1, o2.clone(), o3.clone());
            let r4 = (o1, o2.clone(), o3.clone(), o4.clone());
            let r5 = (o1, o2.clone(), o3.clone(), o4.clone(), o1);
            let r6 = (o1, o2.clone(), o3.clone(), o4.clone(), o1, o2.clone());
            let r7 = (o1, o2.clone(), o3.clone(), o4.clone(), o1, o2.clone(), o3.clone());
            let r8 = (o1, o2.clone(), o3.clone(), o4.clone(),
                      o1, o2.clone(), o3.clone(), o4.clone());
            let r9 = (o1, o2.clone(), o3.clone(), o4.clone(),
                      o1, o2.clone(), o3.clone(), o4.clone(),
                      o1);
            let r10 = (o1, o2.clone(), o3.clone(), o4.clone(),
                       o1, o2.clone(), o3.clone(), o4.clone(),
                       o1, o2.clone());
            let r11 = (o1, o2.clone(), o3.clone(), o4.clone(),
                       o1, o2.clone(), o3.clone(), o4.clone(),
                       o1, o2.clone(), o3.clone());
            let r12 = (o1, o2.clone(), o3.clone(), o4.clone(),
                       o1, o2.clone(), o3.clone(), o4.clone(),
                       o1, o2.clone(), o3.clone(), o4.clone());

            assert_eq!(o1, from_row::<u8>(Row::new(vec![t1], Arc::new(vec![col.clone()]))));
            assert_eq!(o2, from_row::<String>(Row::new(vec![t2], Arc::new(vec![col.clone()]))));
            assert_eq!(o3, from_row::<Vec<u8>>(Row::new(vec![t3], Arc::new(vec![col.clone()]))));
            assert_eq!(o4, from_row::<Timespec>(Row::new(vec![t4], Arc::new(vec![col.clone()]))));
            assert_eq!(r1, from_row(Row::new(v1, Arc::new(repeat(col.clone()).take(1).collect::<Vec<Column>>()))));
            assert_eq!(r2, from_row(Row::new(v2, Arc::new(repeat(col.clone()).take(2).collect::<Vec<Column>>()))));
            assert_eq!(r3, from_row(Row::new(v3, Arc::new(repeat(col.clone()).take(3).collect::<Vec<Column>>()))));
            assert_eq!(r4, from_row(Row::new(v4, Arc::new(repeat(col.clone()).take(4).collect::<Vec<Column>>()))));
            assert_eq!(r5, from_row(Row::new(v5, Arc::new(repeat(col.clone()).take(5).collect::<Vec<Column>>()))));
            assert_eq!(r6, from_row(Row::new(v6, Arc::new(repeat(col.clone()).take(6).collect::<Vec<Column>>()))));
            assert_eq!(r7, from_row(Row::new(v7, Arc::new(repeat(col.clone()).take(7).collect::<Vec<Column>>()))));
            assert_eq!(r8, from_row(Row::new(v8, Arc::new(repeat(col.clone()).take(8).collect::<Vec<Column>>()))));
            assert_eq!(r9, from_row(Row::new(v9, Arc::new(repeat(col.clone()).take(9).collect::<Vec<Column>>()))));
            assert_eq!(r10, from_row(Row::new(v10, Arc::new(repeat(col.clone()).take(10).collect::<Vec<Column>>()))));
            assert_eq!(r11, from_row(Row::new(v11, Arc::new(repeat(col.clone()).take(11).collect::<Vec<Column>>()))));
            assert_eq!(r12, from_row(Row::new(v12, Arc::new(repeat(col.clone()).take(12).collect::<Vec<Column>>()))));
        }

        #[test]
        fn should_return_error_if_could_not_convert() {
            let t1 = Value::Int(1);
            let t2 = Value::Bytes(b"a".to_vec());
            let t3 = Value::Bytes(vec![255]);
            let t4 = Value::Date(2014, 11, 1, 18, 33, 00, 1);

            let v1 = Row::new(vec![t1.clone()], Arc::new(vec![]));
            let v2 = Row::new(vec![t1.clone(), t2.clone()], Arc::new(vec![]));
            let v3 = Row::new(vec![t1.clone(), t2.clone(), t3.clone()], Arc::new(vec![]));
            let v4 = Row::new(vec![t1.clone(), t2.clone(), t3.clone(), t4.clone()], Arc::new(vec![]));
            let v5 = Row::new(vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(), t1.clone()], Arc::new(vec![]));
            let v6 = Row::new(vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                              t1.clone(), t2.clone()], Arc::new(vec![]));
            let v7 = Row::new(vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                              t1.clone(), t2.clone(), t3.clone()], Arc::new(vec![]));
            let v8 = Row::new(vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                              t1.clone(), t2.clone(), t3.clone(), t4.clone()], Arc::new(vec![]));
            let v9 = Row::new(vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                              t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                              t1.clone()], Arc::new(vec![]));
            let v10 = Row::new(vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                               t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                               t1.clone(), t2.clone()], Arc::new(vec![]));
            let v11 = Row::new(vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                               t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                               t1.clone(), t2.clone(), t3.clone()], Arc::new(vec![]));
            let v12 = Row::new(vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                               t1.clone(), t2.clone(), t3.clone(), t4.clone(),
                               t1.clone(), t2.clone(), t3.clone(), t4.clone()], Arc::new(vec![]));

            match from_row_opt::<f32>(v1.clone()) {
                Err(Error::FromRowError(e)) => assert_eq!(v1, e),
                _ => unreachable!(),
            }
            match from_row_opt::<(f32,u8)>(v2.clone()) {
                Err(Error::FromRowError(e)) => assert_eq!(v2, e),
                _ => unreachable!(),
            }
            match from_row_opt::<(f32,u8,u8)>(v3.clone()) {
                Err(Error::FromRowError(e)) => assert_eq!(v3, e),
                _ => unreachable!(),
            }
            match from_row_opt::<(f32,u8,u8,u8)>(v4.clone()) {
                Err(Error::FromRowError(e)) => assert_eq!(v4, e),
                _ => unreachable!(),
            }
            match from_row_opt::<(f32,u8,u8,u8,u8)>(v5.clone()) {
                Err(Error::FromRowError(e)) => assert_eq!(v5, e),
                _ => unreachable!(),
            }
            match from_row_opt::<(f32,u8,u8,u8,u8,u8)>(v6.clone()) {
                Err(Error::FromRowError(e)) => assert_eq!(v6, e),
                _ => unreachable!(),
            }
            match from_row_opt::<(f32,u8,u8,u8,u8,u8,u8)>(v7.clone()) {
                Err(Error::FromRowError(e)) => assert_eq!(v7, e),
                _ => unreachable!(),
            }
            match from_row_opt::<(f32,u8,u8,u8,u8,u8,u8,u8)>(v8.clone()) {
                Err(Error::FromRowError(e)) => assert_eq!(v8, e),
                _ => unreachable!(),
            }
            match from_row_opt::<(f32,u8,u8,u8,u8,u8,u8,u8,u8)>(v9.clone()) {
                Err(Error::FromRowError(e)) => assert_eq!(v9, e),
                _ => unreachable!(),
            }
            match from_row_opt::<(f32,u8,u8,u8,u8,u8,u8,u8,u8,u8)>(v10.clone()) {
                Err(Error::FromRowError(e)) => assert_eq!(v10, e),
                _ => unreachable!(),
            }
            match from_row_opt::<(f32,u8,u8,u8,u8,u8,u8,u8,u8,u8,u8)>(v11.clone()) {
                Err(Error::FromRowError(e)) => assert_eq!(v11, e),
                _ => unreachable!(),
            }
            match from_row_opt::<(f32,u8,u8,u8,u8,u8,u8,u8,u8,u8,u8,u8)>(v12.clone()) {
                Err(Error::FromRowError(e)) => assert_eq!(v12, e),
                _ => unreachable!(),
            }
        }
    }

    #[cfg(feature = "nightly")]
    mod bench {
        use Column;
        use std::iter::repeat;
        use std::sync::Arc;
        use test;
        use super::super::{from_row, from_value, Value};
        use super::super::super::conn::Row;
        use chrono::NaiveDateTime;

        #[bench]
        fn pop_vs_from_row_pop(bencher: &mut test::Bencher) {
            let row = vec![Value::Int(1),
                           Value::Bytes(vec![b'a']),
                           Value::Bytes(vec![255]),
                           Value::Date(2014, 11, 1, 18, 33, 00, 1),
                           Value::Int(1),
                           Value::Bytes(vec![b'a']),
                           Value::Bytes(vec![255]),
                           Value::Date(2014, 11, 1, 18, 33, 00, 1),
                           Value::Int(1),
                           Value::Bytes(vec![b'a']),
                           Value::Bytes(vec![255]),
                           Value::Date(2014, 11, 1, 18, 33, 00, 1)];
            bencher.iter(|| {
                let mut row = row.clone();
                let v12 = from_value::<NaiveDateTime>(row.pop().unwrap());
                let v11 = from_value::<Vec<u8>>(row.pop().unwrap());
                let v10 = from_value::<String>(row.pop().unwrap());
                let v9 = from_value::<i8>(row.pop().unwrap());
                let v8 = from_value::<NaiveDateTime>(row.pop().unwrap());
                let v7 = from_value::<Vec<u8>>(row.pop().unwrap());
                let v6 = from_value::<String>(row.pop().unwrap());
                let v5 = from_value::<i8>(row.pop().unwrap());
                let v4 = from_value::<NaiveDateTime>(row.pop().unwrap());
                let v3 = from_value::<Vec<u8>>(row.pop().unwrap());
                let v2 = from_value::<String>(row.pop().unwrap());
                let v1 = from_value::<i8>(row.pop().unwrap());
                test::black_box((v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12));
            });
        }

        #[bench]
        fn pop_vs_from_row_from_row(bencher: &mut test::Bencher) {
            let col = Column::from_payload(b"\x03def\x06schema\x05table\x09org_table\x04name\x08\
                                           org_name\x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\
                                           \x00\x00".to_vec()).unwrap();
            let row = Row::new(vec![Value::Int(1),
                                    Value::Bytes(vec![b'a']),
                                    Value::Bytes(vec![255]),
                                    Value::Date(2014, 11, 1, 18, 33, 00, 1),
                                    Value::Int(1),
                                    Value::Bytes(vec![b'a']),
                                    Value::Bytes(vec![255]),
                                    Value::Date(2014, 11, 1, 18, 33, 00, 1),
                                    Value::Int(1),
                                    Value::Bytes(vec![b'a']),
                                    Value::Bytes(vec![255]),
                                    Value::Date(2014, 11, 1, 18, 33, 00, 1)],
                               Arc::new(repeat(col).take(12).collect::<Vec<Column>>()));
            bencher.iter(|| {
                let vs: (i8, String, Vec<u8>, NaiveDateTime,
                         i8, String, Vec<u8>, NaiveDateTime,
                         i8, String, Vec<u8>, NaiveDateTime) = from_row(row.clone());
                test::black_box(vs);
            });
        }
    }
}
