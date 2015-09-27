use std::mem;
use std::str::FromStr;
use std::str::from_utf8;
use std::borrow::ToOwned;
use std::io;
use std::io::Write as stdWrite;
use std::time::Duration;
use time::{Tm, Timespec, now, strptime, at};

use super::consts;
use super::conn::{Column};
use super::io::{Write, Read};

use regex::Regex;

use byteorder::LittleEndian as LE;
use byteorder::{ByteOrder, WriteBytesExt};

lazy_static! {
    static ref TM_UTCOFF: i32 = now().tm_utcoff;
    static ref TM_ISDST: i32 = now().tm_isdst;
}

/// A way to store negativeness of mysql's time. `.0 == true` means negative.
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub struct SignedDuration(pub bool, pub Duration);

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
/// To convert something to `Value` you should implement
/// [`IntoValue`](trait.IntoValue.html) on it.
///
/// ```rust
/// # use mysql::conn::pool;
/// # use mysql::conn::MyOpts;
/// use mysql::value::from_row;
/// # use std::thread::Thread;
/// # use std::default::Default;
/// # fn get_opts() -> MyOpts {
/// #     let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or("password".to_string());
/// #     let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
/// #                                .map(|my_port| my_port.parse().ok().unwrap_or(3307))
/// #                                .unwrap_or(3307);
/// #     MyOpts {
/// #         user: Some("root".to_string()),
/// #         pass: Some(pwd),
/// #         tcp_addr: Some("127.0.0.1".to_string()),
/// #         tcp_port: port,
/// #         ..Default::default()
/// #     }
/// # }
/// # let opts = get_opts();
/// # let pool = pool::MyPool::new(opts).unwrap();
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
                try!(writer.write_lenenc_bytes(&x[..]));
            },
            Value::Int(x) => {
                try!(writer.write_i64::<LE>(x));
            },
            Value::UInt(x) => {
                try!(writer.write_u64::<LE>(x));
            },
            Value::Float(x) => {
                try!(writer.write_f64::<LE>(x));
            },
            Value::Date(0u16, 0u8, 0u8, 0u8, 0u8, 0u8, 0u32) => {
                try!(writer.write_u8(0u8));
            },
            Value::Date(y, m, d, 0u8, 0u8, 0u8, 0u32) => {
                try!(writer.write_u8(4u8));
                try!(writer.write_u16::<LE>(y));
                try!(writer.write_u8(m));
                try!(writer.write_u8(d));
            },
            Value::Date(y, m, d, h, i, s, 0u32) => {
                try!(writer.write_u8(7u8));
                try!(writer.write_u16::<LE>(y));
                try!(writer.write_u8(m));
                try!(writer.write_u8(d));
                try!(writer.write_u8(h));
                try!(writer.write_u8(i));
                try!(writer.write_u8(s));
            },
            Value::Date(y, m, d, h, i, s, u) => {
                try!(writer.write_u8(11u8));
                try!(writer.write_u16::<LE>(y));
                try!(writer.write_u8(m));
                try!(writer.write_u8(d));
                try!(writer.write_u8(h));
                try!(writer.write_u8(i));
                try!(writer.write_u8(s));
                try!(writer.write_u32::<LE>(u));
            },
            Value::Time(_, 0u32, 0u8, 0u8, 0u8, 0u32) => try!(writer.write_u8(0u8)),
            Value::Time(neg, d, h, m, s, 0u32) => {
                try!(writer.write_u8(8u8));
                try!(writer.write_u8(if neg {1u8} else {0u8}));
                try!(writer.write_u32::<LE>(d));
                try!(writer.write_u8(h));
                try!(writer.write_u8(m));
                try!(writer.write_u8(s));
            },
            Value::Time(neg, d, h, m, s, u) => {
                try!(writer.write_u8(12u8));
                try!(writer.write_u8(if neg {1u8} else {0u8}));
                try!(writer.write_u32::<LE>(d));
                try!(writer.write_u8(h));
                try!(writer.write_u8(m));
                try!(writer.write_u8(s));
                try!(writer.write_u32::<LE>(u));
            }
        };
        Ok(writer)
    }

    #[doc(hidden)]
    pub fn from_payload(pld: &[u8], columns_count: usize) -> io::Result<Vec<Value>> {
        let mut output = Vec::with_capacity(columns_count);
        let mut reader = io::Cursor::new(pld);
        loop {
            if reader.get_ref().len() as u64 == reader.position() {
                break;
            } else if pld[reader.position() as usize] == 0xfb {
                let new_position = reader.position() + 1;
                reader.set_position(new_position);
                output.push(Value::NULL);
            } else {
                output.push(Value::Bytes(try!(reader.read_lenenc_bytes())));
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
        for i in (0..bitmap_len) {
            bitmap.push(pld[i+1]);
        }
        let mut reader = &pld[1 + bitmap_len..];
        for i in 0..columns.len() {
            let c = &columns[i];
            if bitmap[(i + bit_offset) / 8] & (1 << ((i + bit_offset) % 8)) == 0 {
                values.push(try!(reader.read_bin_value(c.column_type,
                                                       c.flags.contains(consts::UNSIGNED_FLAG))));
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
                    let val = try!(value.to_bin());
                    if val.len() < cap - written {
                        written += val.len();
                        try!(writer.write_all(&val[..]));
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

/// Will *panic* if could not convert `row` to `T`.
#[inline]
pub fn from_row<T: FromRow>(row: Vec<Value>) -> T {
    FromRow::from_row(row)
}

/// Will return `Err(row)` if could not convert `row` to `T`
#[inline]
pub fn from_row_opt<T: FromRow>(row: Vec<Value>) -> Result<T, Vec<Value>> {
    FromRow::from_row_opt(row)
}

/// Trait to convert `Vec<Value>` into tuple of `FromValue` implementors up to arity 12.
pub trait FromRow {
    /// Will *panic* if could not convert `row` to `Self`.
    fn from_row(row: Vec<Value>) -> Self;
    /// Will return `Err(row)` if could not convert `row` to `Self`
    fn from_row_opt(row: Vec<Value>) -> Result<Self, Vec<Value>> where Self: Sized;
}

impl<T: FromValue> FromRow for T {
    fn from_row(row: Vec<Value>) -> T {
        FromRow::from_row_opt(row).ok().expect("Could not convert row to T")
    }
    fn from_row_opt(mut row: Vec<Value>) -> Result<T, Vec<Value>> {
        if row.len() == 1 {
            match from_value_opt(row.pop().unwrap()) {
                Ok(t) => Ok(t),
                v => {
                    mem::forget(v);
                    unsafe { row.set_len(1); }
                    Err(row)
                }
            }
        } else {
            Err(row)
        }
    }
}

impl<T1: FromValue> FromRow for (T1,) {
    #[inline]
    fn from_row(row: Vec<Value>) -> (T1,) {
        FromRow::from_row_opt(row).ok().expect("Could not convert row to (T1,)")
    }
    fn from_row_opt(mut row: Vec<Value>) -> Result<(T1,), Vec<Value>> {
        if row.len() == 1 {
            match from_value_opt(row.pop().unwrap()) {
                Ok(t1) => Ok((t1,)),
                v1 => {
                    mem::forget(v1);
                    unsafe { row.set_len(1); }
                    Err(row)
                }
            }
        } else {
            Err(row)
        }
    }
}

impl<T1: FromValue,
     T2: FromValue> FromRow for (T1, T2) {
    #[inline]
    fn from_row(row: Vec<Value>) -> (T1, T2) {
        FromRow::from_row_opt(row).ok().expect("Could not convert row to (T1, T2)")
    }
    fn from_row_opt(mut row: Vec<Value>) -> Result<(T1, T2), Vec<Value>> {
        if row.len() == 2 {
            let vs = (from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()));
            match vs {
                (Ok(t2), Ok(t1)) =>  {
                    Ok((t1, t2))
                },
                vs => {
                    mem::forget(vs);
                    unsafe { row.set_len(2); }
                    Err(row)
                }
            }
        } else {
            Err(row)
        }
    }
}

impl<T1: FromValue,
     T2: FromValue,
     T3: FromValue> FromRow for (T1, T2, T3) {
    #[inline]
    fn from_row(row: Vec<Value>) -> (T1, T2, T3) {
        FromRow::from_row_opt(row)
        .ok().expect("Could not convert row to (T1, T2, T3)")
    }
    fn from_row_opt(mut row: Vec<Value>) -> Result<(T1, T2, T3), Vec<Value>> {
        if row.len() == 3 {
            let vs = (from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()));
            match vs {
                (Ok(t3), Ok(t2), Ok(t1)) =>  {
                    Ok((t1, t2, t3))
                },
                vs => {
                    mem::forget(vs);
                    unsafe { row.set_len(3); }
                    Err(row)
                }
            }
        } else {
            Err(row)
        }
    }
}

impl<T1: FromValue,
     T2: FromValue,
     T3: FromValue,
     T4: FromValue> FromRow for (T1, T2, T3, T4) {
    #[inline]
    fn from_row(row: Vec<Value>) -> (T1, T2, T3, T4) {
        FromRow::from_row_opt(row)
        .ok().expect("Could not convert row to (T1, T2, T3, T4)")
    }
    fn from_row_opt(mut row: Vec<Value>) -> Result<(T1, T2, T3, T4), Vec<Value>> {
        if row.len() == 4 {
            let vs = (from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()));
            match vs {
                (Ok(t4), Ok(t3), Ok(t2), Ok(t1)) =>  {
                    Ok((t1, t2, t3, t4))
                },
                vs => {
                    mem::forget(vs);
                    unsafe { row.set_len(4); }
                    Err(row)
                }
            }
        } else {
            Err(row)
        }
    }
}

impl<T1: FromValue,
     T2: FromValue,
     T3: FromValue,
     T4: FromValue,
     T5: FromValue> FromRow for (T1, T2, T3, T4, T5) {
    #[inline]
    fn from_row(row: Vec<Value>) -> (T1, T2, T3, T4, T5) {
        FromRow::from_row_opt(row)
        .ok().expect("Could not convert row to (T1, T2, T3, T4, T5)")
    }
    fn from_row_opt(mut row: Vec<Value>) -> Result<(T1, T2, T3, T4, T5), Vec<Value>> {
        if row.len() == 5 {
            let vs = (from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()));
            match vs {
                (Ok(t5), Ok(t4), Ok(t3), Ok(t2), Ok(t1)) =>  {
                    Ok((t1, t2, t3, t4, t5))
                },
                vs => {
                    mem::forget(vs);
                    unsafe { row.set_len(5); }
                    Err(row)
                }
            }
        } else {
            Err(row)
        }
    }
}

impl<T1: FromValue,
     T2: FromValue,
     T3: FromValue,
     T4: FromValue,
     T5: FromValue,
     T6: FromValue> FromRow for (T1, T2, T3, T4, T5, T6) {
    #[inline]
    fn from_row(row: Vec<Value>) -> (T1, T2, T3, T4, T5, T6) {
        FromRow::from_row_opt(row)
        .ok().expect("Could not convert row to (T1, T2, T3, T4, T5, T6)")
    }
    fn from_row_opt(mut row: Vec<Value>) -> Result<(T1, T2, T3, T4, T5, T6), Vec<Value>> {
        if row.len() == 6 {
            let vs = (from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()));
            match vs {
                (Ok(t6), Ok(t5), Ok(t4), Ok(t3), Ok(t2), Ok(t1)) =>  {
                    Ok((t1, t2, t3, t4, t5, t6))
                },
                vs => {
                    mem::forget(vs);
                    unsafe { row.set_len(6); }
                    Err(row)
                }
            }
        } else {
            Err(row)
        }
    }
}

impl<T1: FromValue,
     T2: FromValue,
     T3: FromValue,
     T4: FromValue,
     T5: FromValue,
     T6: FromValue,
     T7: FromValue> FromRow for (T1, T2, T3, T4, T5, T6, T7) {
    #[inline]
    fn from_row(row: Vec<Value>) -> (T1, T2, T3, T4, T5, T6, T7) {
        FromRow::from_row_opt(row)
        .ok().expect("Could not convert row to (T1, T2, T3, T4, T5, T6, T7)")
    }
    fn from_row_opt(mut row: Vec<Value>) -> Result<(T1, T2, T3, T4, T5, T6, T7), Vec<Value>> {
        if row.len() == 7 {
            let vs = (from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()));
            match vs {
                (Ok(t7), Ok(t6), Ok(t5), Ok(t4), Ok(t3), Ok(t2), Ok(t1)) =>  {
                    Ok((t1, t2, t3, t4, t5, t6, t7))
                },
                vs => {
                    mem::forget(vs);
                    unsafe { row.set_len(7); }
                    Err(row)
                }
            }
        } else {
            Err(row)
        }
    }
}

impl<T1: FromValue,
     T2: FromValue,
     T3: FromValue,
     T4: FromValue,
     T5: FromValue,
     T6: FromValue,
     T7: FromValue,
     T8: FromValue> FromRow for (T1, T2, T3, T4, T5, T6, T7, T8) {
    #[inline]
    fn from_row(row: Vec<Value>) -> (T1, T2, T3, T4, T5, T6, T7, T8) {
        FromRow::from_row_opt(row)
        .ok().expect("Could not convert row to (T1, T2, T3, T4, T5, T6, T7, T8)")
    }
    fn from_row_opt(mut row: Vec<Value>) -> Result<(T1, T2, T3, T4, T5, T6, T7, T8), Vec<Value>> {
        if row.len() == 8 {
            let vs = (from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()));
            match vs {
                (Ok(t8), Ok(t7), Ok(t6), Ok(t5), Ok(t4), Ok(t3), Ok(t2), Ok(t1)) =>  {
                    Ok((t1, t2, t3, t4, t5, t6, t7, t8))
                },
                vs => {
                    mem::forget(vs);
                    unsafe { row.set_len(8); }
                    Err(row)
                }
            }
        } else {
            Err(row)
        }
    }
}

impl<T1: FromValue,
     T2: FromValue,
     T3: FromValue,
     T4: FromValue,
     T5: FromValue,
     T6: FromValue,
     T7: FromValue,
     T8: FromValue,
     T9: FromValue> FromRow for (T1, T2, T3, T4, T5, T6, T7, T8, T9) {
    #[inline]
    fn from_row(row: Vec<Value>) -> (T1, T2, T3, T4, T5, T6, T7, T8, T9) {
        FromRow::from_row_opt(row)
        .ok().expect("Could not convert row to (T1, T2, T3, T4, T5, T6, T7, T8, T9)")
    }
    fn from_row_opt(mut row: Vec<Value>) -> Result<(T1, T2, T3, T4, T5, T6, T7, T8, T9), Vec<Value>> {
        if row.len() == 9 {
            let vs = (from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()));
            match vs {
                (Ok(t9), Ok(t8), Ok(t7), Ok(t6), Ok(t5), Ok(t4), Ok(t3), Ok(t2), Ok(t1)) =>  {
                    Ok((t1, t2, t3, t4, t5, t6, t7, t8, t9))
                },
                vs => {
                    mem::forget(vs);
                    unsafe { row.set_len(9); }
                    Err(row)
                }
            }
        } else {
            Err(row)
        }
    }
}

impl<T1: FromValue,
     T2: FromValue,
     T3: FromValue,
     T4: FromValue,
     T5: FromValue,
     T6: FromValue,
     T7: FromValue,
     T8: FromValue,
     T9: FromValue,
     T10: FromValue> FromRow for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10) {
    #[inline]
    fn from_row(row: Vec<Value>) -> (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10) {
        FromRow::from_row_opt(row)
        .ok().expect("Could not convert row to (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)")
    }
    fn from_row_opt(mut row: Vec<Value>) -> Result<(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10), Vec<Value>> {
        if row.len() == 10 {
            let vs = (from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()));
            match vs {
                (Ok(t10), Ok(t9), Ok(t8), Ok(t7), Ok(t6), Ok(t5), Ok(t4), Ok(t3), Ok(t2), Ok(t1)) =>  {
                    Ok((t1, t2, t3, t4, t5, t6, t7, t8, t9, t10))
                },
                vs => {
                    mem::forget(vs);
                    unsafe { row.set_len(10); }
                    Err(row)
                }
            }
        } else {
            Err(row)
        }
    }
}

impl<T1: FromValue,
     T2: FromValue,
     T3: FromValue,
     T4: FromValue,
     T5: FromValue,
     T6: FromValue,
     T7: FromValue,
     T8: FromValue,
     T9: FromValue,
     T10: FromValue,
     T11: FromValue> FromRow for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11) {
    #[inline]
    fn from_row(row: Vec<Value>) -> (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11) {
        FromRow::from_row_opt(row)
        .ok().expect("Could not convert row to (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)")
    }
    fn from_row_opt(mut row: Vec<Value>) -> Result<(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11), Vec<Value>> {
        if row.len() == 11 {
            let vs = (from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()));
            match vs {
                (Ok(t11), Ok(t10), Ok(t9), Ok(t8), Ok(t7), Ok(t6), Ok(t5), Ok(t4), Ok(t3), Ok(t2), Ok(t1)) =>  {
                    Ok((t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11))
                },
                vs => {
                    mem::forget(vs);
                    unsafe { row.set_len(11); }
                    Err(row)
                }
            }
        } else {
            Err(row)
        }
    }
}

impl<T1: FromValue,
     T2: FromValue,
     T3: FromValue,
     T4: FromValue,
     T5: FromValue,
     T6: FromValue,
     T7: FromValue,
     T8: FromValue,
     T9: FromValue,
     T10: FromValue,
     T11: FromValue,
     T12: FromValue> FromRow for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12) {
    #[inline]
    fn from_row(row: Vec<Value>) -> (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12) {
        FromRow::from_row_opt(row)
        .ok().expect("Could not convert row to (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)")
    }
    fn from_row_opt(mut row: Vec<Value>) -> Result<(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12), Vec<Value>> {
        if row.len() == 12 {
            let vs = (from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()),
                      from_value_opt(row.pop().unwrap()));
            match vs {
                (Ok(t12), Ok(t11), Ok(t10), Ok(t9), Ok(t8), Ok(t7), Ok(t6), Ok(t5), Ok(t4), Ok(t3), Ok(t2), Ok(t1)) =>  {
                    Ok((t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12))
                },
                vs => {
                    mem::forget(vs);
                    unsafe { row.set_len(12); }
                    Err(row)
                }
            }
        } else {
            Err(row)
        }
    }
}

pub trait ToRow {
    fn to_row(self) -> Vec<Value>;
}

impl<'a, T: ToRow + Clone> ToRow for &'a T {
    fn to_row(self) -> Vec<Value> {
        self.clone().to_row()
    }
}

impl ToRow for Vec<Value> {
    fn to_row(self) -> Vec<Value> {
        self
    }
}

impl<'a> ToRow for &'a [&'a ToValue] {
    fn to_row(self) -> Vec<Value> {
        let mut row: Vec<Value> = Vec::with_capacity(self.len());
        for v in self.into_iter() {
            row.push(v.to_value());
        }
        row
    }
}

impl ToRow for () {
    fn to_row(self) -> Vec<Value> {
        Vec::new()
    }
}

impl<A: IntoValue> ToRow for (A,) {
    fn to_row(self) -> Vec<Value> {
        let (a,) = self;
        vec![a.into_value()]
    }
}

impl<A: IntoValue,
     B: IntoValue> ToRow for (A, B) {
    fn to_row(self) -> Vec<Value> {
        let (a, b) = self;
        vec![a.into_value(), b.into_value()]
    }
}

impl<A: IntoValue,
     B: IntoValue,
     C: IntoValue> ToRow for (A, B, C) {
    fn to_row(self) -> Vec<Value> {
        let (a, b, c) = self;
        vec![a.into_value(), b.into_value(), c.into_value()]
    }
}

impl<A: IntoValue,
     B: IntoValue,
     C: IntoValue,
     D: IntoValue> ToRow for (A, B, C, D) {
    fn to_row(self) -> Vec<Value> {
        let (a, b, c, d) = self;
        vec![a.into_value(), b.into_value(), c.into_value(), d.into_value()]
    }
}

impl<A: IntoValue,
     B: IntoValue,
     C: IntoValue,
     D: IntoValue,
     E: IntoValue> ToRow for (A, B, C, D, E) {
    fn to_row(self) -> Vec<Value> {
        let (a, b, c, d, e) = self;
        vec![a.into_value(), b.into_value(), c.into_value(), d.into_value(), e.into_value()]
    }
}

impl<A: IntoValue,
     B: IntoValue,
     C: IntoValue,
     D: IntoValue,
     E: IntoValue,
     F: IntoValue> ToRow for (A, B, C, D, E, F) {
    fn to_row(self) -> Vec<Value> {
        let (a, b, c, d, e, f) = self;
        vec![a.into_value(), b.into_value(), c.into_value(), d.into_value(), e.into_value(), f.into_value()]
    }
}

impl<A: IntoValue,
     B: IntoValue,
     C: IntoValue,
     D: IntoValue,
     E: IntoValue,
     F: IntoValue,
     G: IntoValue> ToRow for (A, B, C, D, E, F, G) {
    fn to_row(self) -> Vec<Value> {
        let (a, b, c, d, e, f, g) = self;
        vec![a.into_value(), b.into_value(), c.into_value(), d.into_value(), e.into_value(), f.into_value(), g.into_value()]
    }
}

impl<A: IntoValue,
     B: IntoValue,
     C: IntoValue,
     D: IntoValue,
     E: IntoValue,
     F: IntoValue,
     G: IntoValue,
     H: IntoValue> ToRow for (A, B, C, D, E, F, G, H) {
    fn to_row(self) -> Vec<Value> {
        let (a, b, c, d, e, f, g, h) = self;
        vec![a.into_value(), b.into_value(), c.into_value(), d.into_value(), e.into_value(), f.into_value(), g.into_value(), h.into_value()]
    }
}

impl<A: IntoValue,
     B: IntoValue,
     C: IntoValue,
     D: IntoValue,
     E: IntoValue,
     F: IntoValue,
     G: IntoValue,
     H: IntoValue,
     I: IntoValue> ToRow for (A, B, C, D, E, F, G, H, I) {
    fn to_row(self) -> Vec<Value> {
        let (a, b, c, d, e, f, g, h, i) = self;
        vec![a.into_value(), b.into_value(), c.into_value(), d.into_value(), e.into_value(), f.into_value(), g.into_value(), h.into_value(), i.into_value()]
    }
}

impl<A: IntoValue,
     B: IntoValue,
     C: IntoValue,
     D: IntoValue,
     E: IntoValue,
     F: IntoValue,
     G: IntoValue,
     H: IntoValue,
     I: IntoValue,
     J: IntoValue> ToRow for (A, B, C, D, E, F, G, H, I, J) {
    fn to_row(self) -> Vec<Value> {
        let (a, b, c, d, e, f, g, h, i, j) = self;
        vec![a.into_value(), b.into_value(), c.into_value(), d.into_value(), e.into_value(), f.into_value(), g.into_value(), h.into_value(), i.into_value(), j.into_value()]
    }
}

impl<A: IntoValue,
     B: IntoValue,
     C: IntoValue,
     D: IntoValue,
     E: IntoValue,
     F: IntoValue,
     G: IntoValue,
     H: IntoValue,
     I: IntoValue,
     J: IntoValue,
     K: IntoValue> ToRow for (A, B, C, D, E, F, G, H, I, J, K) {
    fn to_row(self) -> Vec<Value> {
        let (a, b, c, d, e, f, g, h, i, j, k) = self;
        vec![a.into_value(), b.into_value(), c.into_value(), d.into_value(), e.into_value(), f.into_value(), g.into_value(), h.into_value(), i.into_value(), j.into_value(), k.into_value()]
    }
}

impl<A: IntoValue,
     B: IntoValue,
     C: IntoValue,
     D: IntoValue,
     E: IntoValue,
     F: IntoValue,
     G: IntoValue,
     H: IntoValue,
     I: IntoValue,
     J: IntoValue,
     K: IntoValue,
     L: IntoValue> ToRow for (A, B, C, D, E, F, G, H, I, J, K, L) {
    fn to_row(self) -> Vec<Value> {
        let (a, b, c, d, e, f, g, h, i, j, k, l) = self;
        vec![a.into_value(), b.into_value(), c.into_value(), d.into_value(), e.into_value(), f.into_value(), g.into_value(), h.into_value(), i.into_value(), j.into_value(), k.into_value(), l.into_value()]
    }
}

pub trait ToValue {
    fn to_value(&self) -> Value;
}

impl<T: IntoValue + Clone> ToValue for T {
    fn to_value(&self) -> Value {
        self.clone().into_value()
    }
}

/// Implement this trait if you want to convert something to `Value`.
pub trait IntoValue {
    fn into_value(self) -> Value;
}

impl<'a, T: ToValue> IntoValue for &'a T {
    fn into_value(self) -> Value {
        self.to_value()
    }
}

impl<T: IntoValue> IntoValue for Option<T> {
    fn into_value(self) -> Value {
        match self {
            None => Value::NULL,
            Some(x) => x.into_value(),
        }
    }
}

macro_rules! into_value_impl_num(
    (i64) => (
        impl IntoValue for i64 {
            fn into_value(self) -> Value {
                Value::Int(self)
            }
        }
    );
    ($t:ty) => (
        impl IntoValue for $t {
            fn into_value(self) -> Value {
                Value::Int(self as i64)
            }
        }
    )
);

into_value_impl_num!(i8);
into_value_impl_num!(u8);
into_value_impl_num!(i16);
into_value_impl_num!(u16);
into_value_impl_num!(i32);
into_value_impl_num!(u32);
into_value_impl_num!(isize);
into_value_impl_num!(i64);

impl IntoValue for u64 {
    fn into_value(self) -> Value {
        Value::UInt(self)
    }
}

impl IntoValue for usize {
    fn into_value(self) -> Value {
        if self as u64 <= ::std::usize::MAX as u64 {
            Value::Int(self as i64)
        } else {
            Value::UInt(self as u64)
        }
    }
}

impl IntoValue for f32 {
    fn into_value(self) -> Value {
        Value::Float(self as f64)
    }
}

impl IntoValue for f64 {
    fn into_value(self) -> Value {
        Value::Float(self)
    }
}

impl IntoValue for bool {
    fn into_value(self) -> Value {
        Value::Int(if self {1} else {0})
    }
}

impl<'a> IntoValue for &'a [u8] {
    fn into_value(self) -> Value {
        Value::Bytes(self.into())
    }
}

impl IntoValue for Vec<u8> {
    fn into_value(self) -> Value {
        Value::Bytes(self)
    }
}

impl<'a> IntoValue for &'a str {
    fn into_value(self) -> Value {
        let string: String = self.into();
        Value::Bytes(string.into_bytes())
    }
}

impl IntoValue for String {
    fn into_value(self) -> Value {
        Value::Bytes(self.into_bytes())
    }
}

impl IntoValue for Timespec {
    fn into_value(self) -> Value {
        let t = at(self);
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

impl IntoValue for Duration {
    fn into_value(self) -> Value {
        let mut secs_total = self.as_secs();
        let micros = (self.subsec_nanos() as f64 / 1000_f64).round() as u32;
        let seconds = (secs_total % 60) as u8;
        secs_total -= seconds as u64;
        let minutes = ((secs_total % (60 * 60)) / 60) as u8;
        secs_total -= (minutes as u64) * 60;
        let hours = ((secs_total % (60 * 60 * 24)) / (60 * 60)) as u8;
        secs_total -= (hours as u64) * 60 * 60;
        Value::Time(false, (secs_total / (60 * 60 * 24)) as u32, hours, minutes, seconds, micros)
    }
}

impl IntoValue for SignedDuration {
    fn into_value(self) -> Value {
        let SignedDuration(is_neg, duration) = self;
        if let Value::Time(_, days, hours, minutes, seconds, micros) = duration.into_value() {
            Value::Time(is_neg, days, hours, minutes, seconds, micros)
        } else {
            unreachable!()
        }
    }
}

impl IntoValue for Value {
    fn into_value(self) -> Value {
        self
    }
}

/// Implement this trait to convert value to something.
pub trait FromValue {
    /// Will panic if could not convert `v` to `Self`
    fn from_value(v: Value) -> Self;

    /// Will return `Err(v)` if could not convert `v` to `Self`
    fn from_value_opt(v: Value) -> Result<Self, Value> where Self: Sized;
}

impl FromValue for Value {
    fn from_value(v: Value) -> Value {
        v
    }
    fn from_value_opt(v: Value) -> Result<Value, Value> {
        Ok(v)
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(v: Value) -> Option<T> {
        match v {
            Value::NULL => None,
            v => Some(FromValue::from_value(v))
        }
    }
    fn from_value_opt(v: Value) -> Result<Option<T>, Value> {
        match v {
            Value::NULL => Ok(None),
            v => {
                match FromValue::from_value_opt(v) {
                    Err(v) => Err(v),
                    Ok(x) => Ok(Some(x)),
                }
            }
        }
    }
}

/// Will panic if could not convert `v` to `T`
#[inline]
pub fn from_value<T: FromValue>(v: Value) -> T {
    FromValue::from_value(v)
}

/// Will return `Err(v)` if could not convert `v` to `T`
#[inline]
pub fn from_value_opt<T: FromValue>(v: Value) -> Result<T, Value> {
    FromValue::from_value_opt(v)
}

macro_rules! from_value_impl_num {
    ($t:ident) => (
        impl FromValue for $t {
            fn from_value(v: Value) -> $t {
                from_value_opt(v).ok().expect("Error retrieving number from value")
            }
            fn from_value_opt(v: Value) -> Result<$t, Value> {
                match v {
                    Value::Int(x) => {
                        let min = ::std::$t::MIN as i64;
                        let mut max = ::std::$t::MAX as i64;
                        if max < 0 {
                            max = ::std::i64::MAX;
                        }
                        if min <= x && x <= max {
                            Ok(x as $t)
                        } else {
                            Err(Value::Int(x))
                        }
                    },
                    Value::UInt(x) if x <= ::std::$t::MAX as u64 => Ok(x as $t),
                    Value::Bytes(bytes) => {
                        from_utf8(&bytes[..]).ok()
                        .and_then(|x| {
                            FromStr::from_str(x).ok()
                        }).ok_or(Value::Bytes(bytes))
                    },
                    v => Err(v),
                }
            }
        }
    )
}

from_value_impl_num!(i8);
from_value_impl_num!(u8);
from_value_impl_num!(i16);
from_value_impl_num!(u16);
from_value_impl_num!(i32);
from_value_impl_num!(u32);
from_value_impl_num!(isize);
from_value_impl_num!(usize);

impl FromValue for i64 {
    fn from_value(v: Value) -> i64 {
        from_value_opt(v).ok().expect("Error retrieving i64 from value")
    }
    fn from_value_opt(v: Value) -> Result<i64, Value> {
        match v {
            Value::Int(x) => Ok(x),
            Value::UInt(x) if x <= ::std::i64::MAX as u64 => Ok(x as i64),
            Value::Bytes(bytes) => {
                from_utf8(&bytes[..]).ok()
                .and_then(|x| {
                    FromStr::from_str(x).ok()
                }).ok_or(Value::Bytes(bytes))
            },
            v => Err(v),
        }
    }
}

impl FromValue for u64 {
    fn from_value(v: Value) -> u64 {
        from_value_opt(v).ok().expect("Error retrieving u64 from value")
    }
    fn from_value_opt(v: Value) -> Result<u64, Value> {
        match v {
            Value::Int(x) if x >= 0 => Ok(x as u64),
            Value::UInt(x) => Ok(x),
            Value::Bytes(bytes) => {
                from_utf8(&bytes[..]).ok()
                .and_then(|x| {
                    FromStr::from_str(x).ok()
                }).ok_or(Value::Bytes(bytes))
            },
            v => Err(v),
        }
    }
}

impl FromValue for f32 {
    fn from_value(v: Value) -> f32 {
        from_value_opt(v).ok().expect("Error retrieving f32 from value")
    }
    fn from_value_opt(v: Value) -> Result<f32, Value> {
        match v {
            Value::Float(x) if x >= ::std::f32::MIN as f64 && x <= ::std::f32::MAX as f64 => Ok(x as f32),
            Value::Bytes(bytes) => {
                from_utf8(&bytes[..]).ok()
                .and_then(|x| {
                    FromStr::from_str(x).ok()
                }).ok_or(Value::Bytes(bytes))
            },
            v => Err(v),
        }
    }
}

impl FromValue for f64 {
    fn from_value(v: Value) -> f64 {
        from_value_opt(v).ok().expect("Error retrieving f64 from value")
    }
    #[inline]
    fn from_value_opt(v: Value) -> Result<f64, Value> {
        match v {
            Value::Float(x) => Ok(x),
            Value::Bytes(bytes) => {
                from_utf8(&bytes[..]).ok()
                .and_then(|x| {
                    FromStr::from_str(x).ok()
                }).ok_or(Value::Bytes(bytes))
            },
            v => Err(v),
        }
    }
}

impl FromValue for bool {
    fn from_value(v: Value) -> bool {
        from_value_opt(v).ok().expect("Error retrieving bool from value")
    }
    fn from_value_opt(v: Value) -> Result<bool, Value> {
        match v {
            Value::Int(0) => Ok(false),
            Value::Int(1) => Ok(true),
            Value::Bytes(ref bts) if bts.len() == 1 && bts[0] == 0x30 => Ok(false),
            Value::Bytes(ref bts) if bts.len() == 1 && bts[0] == 0x31 => Ok(true),
            v => Err(v),
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_value(v: Value) -> Vec<u8> {
        from_value_opt(v).ok().expect("Error retrieving Vec<u8> from value")
    }
    fn from_value_opt(v: Value) -> Result<Vec<u8>, Value> {
        match v {
            Value::Bytes(bts) => Ok(bts),
            v => Err(v),
        }
    }
}

impl FromValue for String {
    fn from_value(v: Value) -> String {
        from_value_opt(v).ok().expect("Error retrieving String from value")
    }
    fn from_value_opt(v: Value) -> Result<String, Value> {
        match v {
            Value::Bytes(bytes) => {
                String::from_utf8(bytes).map_err(|e| Value::Bytes(e.into_bytes()))
            },
            v => Err(v),
        }
    }
}

impl FromValue for Timespec {
    fn from_value(v: Value) -> Timespec {
        from_value_opt(v).ok().expect("Error retrieving Timespec from Value")
    }
    fn from_value_opt(v: Value) -> Result<Timespec, Value> {
        match v {
            Value::Date(y, m, d, h, i, s, u) => {
                Ok(Tm{
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
                    }.to_timespec())
            },
            Value::Bytes(bytes) => {
                from_utf8(&bytes[..]).ok()
                .and_then(|s| {
                    strptime(s, "%Y-%m-%d %H:%M:%S")
                    .or(strptime(s, "%Y-%m-%d"))
                    .ok()
                }).map(|mut tm| {
                    tm.tm_utcoff = *TM_UTCOFF;
                    tm.tm_isdst = *TM_ISDST;
                    tm.to_timespec()
                }).ok_or(Value::Bytes(bytes))
            },
            v => Err(v),
        }
    }
}

impl FromValue for SignedDuration {
    fn from_value(v: Value) -> SignedDuration {
        from_value_opt(v).ok().expect("Error retrieving Duration from Value")
    }
    fn from_value_opt(v: Value) -> Result<SignedDuration, Value> {
        match v {
            Value::Time(is_neg, days, hours, minutes, seconds, micros) => {
                let mut secs_total = seconds as u64;
                secs_total += minutes as u64 * 60;
                secs_total += hours as u64 * 60 * 60;
                secs_total += days as u64 * 24 * 60 * 60;
                Ok(SignedDuration(is_neg, Duration::new(secs_total, micros * 1000)))
            },
            Value::Bytes(val_bytes) => {
                {
                    let mut bytes = &val_bytes[..];
                    let is_neg = bytes[0] == b'-';
                    if is_neg {
                        bytes = &bytes[1..];
                    }
                    from_utf8(bytes).ok()
                    .and_then(|time_str| {
                        let t_ref = time_str.as_ref();
                        Regex::new(r"^([0-8]\d\d):([0-5]\d):([0-5]\d)$").unwrap().captures(t_ref)
                        .or_else(|| {
                            Regex::new(r"^([0-8]\d\d):([0-5]\d):([0-5]\d)\.(\d{1,6})$")
                                  .unwrap()
                                  .captures(t_ref)
                        }).or_else(|| {
                            Regex::new(r"^(\d{2}):([0-5]\d):([0-5]\d)$")
                                  .unwrap()
                                  .captures(t_ref)
                        }).or_else(|| {
                            Regex::new(r"^(\d{2}):([0-5]\d):([0-5]\d)\.(\d{1,6})$")
                                  .unwrap()
                                  .captures(t_ref)
                        })
                    }).and_then(|capts| {
                        let hours = capts.at(1).unwrap().parse::<u64>().unwrap();
                        let minutes = capts.at(2).unwrap().parse::<u64>().unwrap();
                        let seconds = capts.at(3).unwrap().parse::<u64>().unwrap();
                        let micros = if capts.len() == 5 {
                            let micros_str = capts.at(4).unwrap();
                            let mut left_zero_count = 0;
                            for b in micros_str.bytes() {
                                if b == b'0' {
                                    left_zero_count += 1;
                                } else {
                                    break;
                                }
                            }
                            let mut micros = capts.at(4).unwrap().parse::<u32>().unwrap();
                            for _ in 0..(6 - left_zero_count - (micros_str.len() - left_zero_count)) {
                                micros *= 10;
                            }
                            micros
                        } else {
                            0
                        };
                        let mut secs_total = seconds;
                        secs_total += minutes * 60;
                        secs_total += hours * 60 * 60;
                        Some(SignedDuration(is_neg, Duration::new(secs_total, micros * 1_000)))
                    })
                }.ok_or(Value::Bytes(val_bytes))
            },
            _ => Err(v),
        }
    }
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
        use std::time::Duration;
        use super::super::{from_value, SignedDuration};
        use super::super::Value::{Bytes, Date, Int};
        use time::{Timespec, now};

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
        fn should_convert_Bytes_to_Duration() {
            assert_eq!(SignedDuration(true, Duration::from_millis(433830500)),
                       from_value(Bytes(b"-120:30:30.5".to_vec())));
            assert_eq!(SignedDuration(true, Duration::from_millis(433830500)),
                       from_value(Bytes(b"-120:30:30.500000".to_vec())));
            assert_eq!(SignedDuration(true, Duration::from_millis(433830005)),
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
        use std::time::Duration;
        use super::super::{IntoValue, SignedDuration};
        use super::super::Value::{Date, Time};
        use time::{Timespec, now};

        #[test]
        fn should_convert_Time_to_Date() {
            let ts = Timespec {
                sec: 1414866780 - now().tm_utcoff as i64,
                nsec: 1000,
            };
            assert_eq!(ts.into_value(), Date(2014, 11, 1, 18, 33, 00, 1));
        }
        #[test]
        fn should_convert_Duration_to_Time() {
            assert_eq!(Duration::from_millis(433830500).into_value(),
                       Time(false, 5, 0, 30, 30, 500000));
            assert_eq!(SignedDuration(true, Duration::from_millis(433830500)).into_value(),
                       Time(true, 5, 0, 30, 30, 500000));
        }
    }

    mod from_row {
        use time::{Timespec, now};
        use super::super::{from_row, from_row_opt, Value};

        #[test]
        fn should_convert_to_tuples() {
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
            let v5 = vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(), t1.clone()];
            let v6 = vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(), t1.clone(), t2.clone()];
            let v7 = vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(), t1.clone(), t2.clone(), t3.clone()];
            let v8 = vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(), t1.clone(), t2.clone(), t3.clone(), t4.clone()];
            let v9 = vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(), t1.clone(), t2.clone(), t3.clone(), t4.clone(), t1.clone()];
            let v10 = vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(), t1.clone(), t2.clone(), t3.clone(), t4.clone(), t1.clone(), t2.clone()];
            let v11 = vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(), t1.clone(), t2.clone(), t3.clone(), t4.clone(), t1.clone(), t2.clone(), t3.clone()];
            let v12 = vec![t1.clone(), t2.clone(), t3.clone(), t4.clone(), t1.clone(), t2.clone(), t3.clone(), t4.clone(), t1.clone(), t2.clone(), t3.clone(), t4.clone()];
            let r1 = (o1,);
            let r2 = (o1, o2.clone());
            let r3 = (o1, o2.clone(), o3.clone());
            let r4 = (o1, o2.clone(), o3.clone(), o4.clone());
            let r5 = (o1, o2.clone(), o3.clone(), o4.clone(), o1);
            let r6 = (o1, o2.clone(), o3.clone(), o4.clone(), o1, o2.clone());
            let r7 = (o1, o2.clone(), o3.clone(), o4.clone(), o1, o2.clone(), o3.clone());
            let r8 = (o1, o2.clone(), o3.clone(), o4.clone(), o1, o2.clone(), o3.clone(), o4.clone());
            let r9 = (o1, o2.clone(), o3.clone(), o4.clone(), o1, o2.clone(), o3.clone(), o4.clone(), o1);
            let r10 = (o1, o2.clone(), o3.clone(), o4.clone(), o1, o2.clone(), o3.clone(), o4.clone(), o1, o2.clone());
            let r11 = (o1, o2.clone(), o3.clone(), o4.clone(), o1, o2.clone(), o3.clone(), o4.clone(), o1, o2.clone(), o3.clone());
            let r12 = (o1, o2.clone(), o3.clone(), o4.clone(), o1, o2.clone(), o3.clone(), o4.clone(), o1, o2.clone(), o3.clone(), o4.clone());

            assert_eq!(o1, from_row(vec![t1]));
            assert_eq!(o2, from_row::<String>(vec![t2]));
            assert_eq!(o3, from_row::<Vec<u8>>(vec![t3]));
            assert_eq!(o4, from_row(vec![t4]));
            assert_eq!(r1, from_row(v1));
            assert_eq!(r2, from_row(v2));
            assert_eq!(r3, from_row(v3));
            assert_eq!(r4, from_row(v4));
            assert_eq!(r5, from_row(v5));
            assert_eq!(r6, from_row(v6));
            assert_eq!(r7, from_row(v7));
            assert_eq!(r8, from_row(v8));
            assert_eq!(r9, from_row(v9));
            assert_eq!(r10, from_row(v10));
            assert_eq!(r11, from_row(v11));
            assert_eq!(r12, from_row(v12));
        }

        #[test]
        fn should_return_error_if_could_not_convert() {
            let v1 = vec![Value::Int(1)];
            let v2 = vec![Value::Bytes(b"foo".to_vec()), Value::Int(1)];
            let v3 = vec![Value::Bytes(b"foo".to_vec()), Value::Int(1), Value::Int(1)];
            let v4 = vec![Value::Bytes(b"foo".to_vec()), Value::Int(1), Value::Int(1), Value::Int(1)];
            let v5 = vec![Value::Bytes(b"foo".to_vec()), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1)];
            let v6 = vec![Value::Bytes(b"foo".to_vec()), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1)];
            let v7 = vec![Value::Bytes(b"foo".to_vec()), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1)];
            let v8 = vec![Value::Bytes(b"foo".to_vec()), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1)];
            let v9 = vec![Value::Bytes(b"foo".to_vec()), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1)];
            let v10 = vec![Value::Bytes(b"foo".to_vec()), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1)];
            let v11 = vec![Value::Bytes(b"foo".to_vec()), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1)];
            let v12 = vec![Value::Bytes(b"foo".to_vec()), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(1)];
            assert_eq!(Err(v1.clone()), from_row_opt::<String>(v1.clone()));
            assert_eq!(Err(v1.clone()), from_row_opt::<(String,)>(v1));
            assert_eq!(Err(v2.clone()), from_row_opt::<(String,String,)>(v2));
            assert_eq!(Err(v3.clone()), from_row_opt::<(String,i8,String,)>(v3));
            assert_eq!(Err(v4.clone()), from_row_opt::<(String,i8,i8,String,)>(v4));
            assert_eq!(Err(v5.clone()), from_row_opt::<(String,i8,i8,i8,String,)>(v5));
            assert_eq!(Err(v6.clone()), from_row_opt::<(String,i8,i8,i8,i8,String,)>(v6));
            assert_eq!(Err(v7.clone()), from_row_opt::<(String,i8,i8,i8,i8,i8,String,)>(v7));
            assert_eq!(Err(v8.clone()), from_row_opt::<(String,i8,i8,i8,i8,i8,i8,String,)>(v8));
            assert_eq!(Err(v9.clone()), from_row_opt::<(String,i8,i8,i8,i8,i8,i8,i8,String,)>(v9));
            assert_eq!(Err(v10.clone()), from_row_opt::<(String,i8,i8,i8,i8,i8,i8,i8,i8,String,)>(v10));
            assert_eq!(Err(v11.clone()), from_row_opt::<(String,i8,i8,i8,i8,i8,i8,i8,i8,i8,String,)>(v11));
            assert_eq!(Err(v12.clone()), from_row_opt::<(String,i8,i8,i8,i8,i8,i8,i8,i8,i8,i8,String,)>(v12));
        }
    }

    #[cfg(feature = "nightly")]
    mod bench {
        use test;
        use time::Timespec;
        use super::super::{from_row, from_value, Value};

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
                let v12 = from_value::<Timespec>(row.pop().unwrap());
                let v11 = from_value::<Vec<u8>>(row.pop().unwrap());
                let v10 = from_value::<String>(row.pop().unwrap());
                let v9 = from_value::<i8>(row.pop().unwrap());
                let v8 = from_value::<Timespec>(row.pop().unwrap());
                let v7 = from_value::<Vec<u8>>(row.pop().unwrap());
                let v6 = from_value::<String>(row.pop().unwrap());
                let v5 = from_value::<i8>(row.pop().unwrap());
                let v4 = from_value::<Timespec>(row.pop().unwrap());
                let v3 = from_value::<Vec<u8>>(row.pop().unwrap());
                let v2 = from_value::<String>(row.pop().unwrap());
                let v1 = from_value::<i8>(row.pop().unwrap());
                test::black_box((v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12));
            });
        }

        #[bench]
        fn pop_vs_from_row_from_row(bencher: &mut test::Bencher) {
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
                let vs: (i8, String, Vec<u8>, Timespec,
                         i8, String, Vec<u8>, Timespec,
                         i8, String, Vec<u8>, Timespec) = from_row(row.clone());
                test::black_box(vs);
            });
        }
    }
}
