use std::io::{MemWriter, BufReader, IoResult, SeekCur};
use std::from_str::{from_str};
use std::str::{from_utf8};
use std::num::{Bounded, pow};
use std::time::{Duration};
use time::{Tm, Timespec, now, strptime, at};
use super::consts;
use super::conn::{Column};
use super::io::{MyWriter, MyReader};


lazy_static! {
    static ref TM_GMTOFF: i32 = now().tm_gmtoff;
    static ref TM_ISDST: i32 = now().tm_isdst;
}


#[deriving(Clone, PartialEq, PartialOrd, Show)]
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
            NULL => "NULL".into_string(),
            Bytes(ref x) => {
                String::from_utf8(x.clone()).ok().map_or_else(|| {
                    let mut s = "0x".into_string();
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
                    format!("'{:s}'", replaced)
                })
            },
            Int(x) => format!("{:d}", x),
            UInt(x) => format!("{:u}", x),
            Float(x) => format!("{:f}", x),
            Date(0, 0, 0, 0, 0, 0, 0) => "''".into_string(),
            Date(y, m, d, 0, 0, 0, 0) => format!("'{:04u}-{:02u}-{:02u}'", y, m, d),
            Date(y, m, d, h, i, s, 0) => format!("'{:04u}-{:02u}-{:02u} {:02u}:{:02u}:{:02u}'", y, m, d, h, i, s),
            Date(y, m, d, h, i, s, u) => format!("'{:04u}-{:02u}-{:02u} {:02u}:{:02u}:{:02u}.{:06u}'", y, m, d, h, i, s, u),
            Time(_, 0, 0, 0, 0, 0) => "''".into_string(),
            Time(neg, d, h, i, s, 0) => {
                if neg {
                    format!("'-{:03u}:{:02u}:{:02u}'", d * 24 + h as u32, i, s)
                } else {
                    format!("'{:03u}:{:02u}:{:02u}'", d * 24 + h as u32, i, s)
                }
            },
            Time(neg, d, h, i, s, u) => {
                if neg {
                    format!("'-{:03u}:{:02u}:{:02u}.{:06u}'",
                            d * 24 + h as u32, i, s, u)
                } else {
                    format!("'{:03u}:{:02u}:{:02u}.{:06u}'",
                            d * 24 + h as u32, i, s, u)
                }
            }
        }
    }
    #[inline]
    pub fn is_bytes(&self) -> bool {
        match *self {
            Bytes(..) => true,
            _ => false
        }
    }
    #[inline]
    pub fn bytes_ref<'a>(&'a self) -> &'a [u8] {
        match *self {
            Bytes(ref x) => x[],
            _ => panic!("Called `Value::bytes_ref()` on non `Bytes` value")
        }
    }
    #[inline]
    pub fn unwrap_bytes(self) -> Vec<u8> {
        match self {
            Bytes(x) => x,
            _ => panic!("Called `Value::unwrap_bytes()` on non `Bytes` value")
        }
    }
    #[inline]
    pub fn unwrap_bytes_or(self, y: Vec<u8>) -> Vec<u8> {
        match self {
            Bytes(x) => x,
            _ => y
        }
    }
    #[inline]
    pub fn is_int(&self) -> bool {
        match *self {
            Int(..) => true,
            _ => false
        }
    }
    #[inline]
    pub fn get_int(&self) -> i64 {
        match *self {
            Int(x) => x,
            _ => panic!("Called `Value::get_int()` on non `Int` value")
        }
    }
    #[inline]
    pub fn get_int_or(&self, y: i64) -> i64 {
        match *self {
            Int(x) => x,
            _ => y
        }
    }
    #[inline]
    pub fn is_uint(&self) -> bool {
        match *self {
            UInt(..) => true,
            _ => false
        }
    }
    #[inline]
    pub fn get_uint(&self) -> u64 {
        match *self {
            UInt(x) => x,
            _ => panic!("Called `Value::get_uint()` on non `UInt` value")
        }
    }
    #[inline]
    pub fn get_uint_or(&self, y: u64) -> u64 {
        match *self {
            UInt(x) => x,
            _ => y
        }
    }
    #[inline]
    pub fn is_float(&self) -> bool {
        match *self {
            Float(..) => true,
            _ => false
        }
    }
    #[inline]
    pub fn get_float(&self) -> f64 {
        match *self {
            Float(x) => x,
            _ => panic!("Called `Value::get_float()` on non `Float` value")
        }
    }
    #[inline]
    pub fn get_float_or(&self, y: f64) -> f64 {
        match *self {
            Float(x) => x,
            _ => y
        }
    }
    #[inline]
    pub fn is_date(&self) -> bool {
        match *self {
            Date(..) => true,
            _ => false
        }
    }
    #[inline]
    pub fn get_year(&self) -> u16 {
        match *self {
            Date(y, _, _, _, _, _, _) => y,
            _ => panic!("Called `Value::get_year()` on non `Date` value")
        }
    }
    #[inline]
    pub fn get_month(&self) -> u8 {
        match *self {
            Date(_, m, _, _, _, _, _) => m,
            _ => panic!("Called `Value::get_month()` on non `Date` value")
        }
    }
    #[inline]
    pub fn get_day(&self) -> u8 {
        match *self {
            Date(_, _, d, _, _, _, _) => d,
            _ => panic!("Called `Value::get_day()` on non `Date` value")
        }
    }
    #[inline]
    pub fn is_time(&self) -> bool {
        match *self {
            Time(..) => true,
            _ => false
        }
    }
    #[inline]
    pub fn is_neg(&self) -> bool {
        match *self {
            Time(false, _, _, _, _, _) => false,
            Time(true, _, _, _, _, _) => true,
            _ => panic!("Called `Value::is_neg()` on non `Time` value")
        }
    }
    #[inline]
    pub fn get_days(&self) -> u32 {
        match *self {
            Time(_, d, _, _, _, _) => d,
            _ => panic!("Called `Value::get_days()` on non `Time` value")
        }
    }
    #[inline]
    pub fn get_hour(&self) -> u8 {
        match *self {
            Date(_, _, _, h, _, _, _) => h,
            Time(_, _, h, _, _, _) => h,
            _ => panic!("Called `Value::get_hour()` on non `Date` nor `Time` value")
        }
    }
    #[inline]
    pub fn get_min(&self) -> u8 {
        match *self {
            Date(_, _, _, _, i, _, _) => i,
            Time(_, _, _, i, _, _) => i,
            _ => panic!("Called `Value::get_min()` on non `Date` nor `Time` value")
        }
    }
    #[inline]
    pub fn get_sec(&self) -> u8 {
        match *self {
            Date(_, _, _, _, _, s, _) => s,
            Time(_, _, _, _, s, _) => s,
            _ => panic!("Called `Value::get_sec()` on non `Date` nor `Time` value")
        }
    }
    #[inline]
    pub fn get_usec(&self) -> u32 {
        match *self {
            Date(_, _, _, _, _, _, u) => u,
            Time(_, _, _, _, _, u) => u,
            _ => panic!("Called `Value::get_usec()` on non `Date` nor `Time` value")
        }
    }
    pub fn to_bin(&self) -> IoResult<Vec<u8>> {
        let mut writer = MemWriter::with_capacity(256);
        match *self {
            NULL => (),
            Bytes(ref x) => {
                try!(writer.write_lenenc_bytes(x[]));
            },
            Int(x) => {
                try!(writer.write_le_i64(x));
            },
            UInt(x) => {
                try!(writer.write_le_u64(x));
            },
            Float(x) => {
                try!(writer.write_le_f64(x));
            },
            Date(0u16, 0u8, 0u8, 0u8, 0u8, 0u8, 0u32) => {
                try!(writer.write_u8(0u8));
            },
            Date(y, m, d, 0u8, 0u8, 0u8, 0u32) => {
                try!(writer.write_u8(4u8));
                try!(writer.write_le_u16(y));
                try!(writer.write_u8(m));
                try!(writer.write_u8(d));
            },
            Date(y, m, d, h, i, s, 0u32) => {
                try!(writer.write_u8(7u8));
                try!(writer.write_le_u16(y));
                try!(writer.write_u8(m));
                try!(writer.write_u8(d));
                try!(writer.write_u8(h));
                try!(writer.write_u8(i));
                try!(writer.write_u8(s));
            },
            Date(y, m, d, h, i, s, u) => {
                try!(writer.write_u8(11u8));
                try!(writer.write_le_u16(y));
                try!(writer.write_u8(m));
                try!(writer.write_u8(d));
                try!(writer.write_u8(h));
                try!(writer.write_u8(i));
                try!(writer.write_u8(s));
                try!(writer.write_le_u32(u));
            },
            Time(_, 0u32, 0u8, 0u8, 0u8, 0u32) => try!(writer.write_u8(0u8)),
            Time(neg, d, h, m, s, 0u32) => {
                try!(writer.write_u8(8u8));
                try!(writer.write_u8(if neg {1u8} else {0u8}));
                try!(writer.write_le_u32(d));
                try!(writer.write_u8(h));
                try!(writer.write_u8(m));
                try!(writer.write_u8(s));
            },
            Time(neg, d, h, m, s, u) => {
                try!(writer.write_u8(12u8));
                try!(writer.write_u8(if neg {1u8} else {0u8}));
                try!(writer.write_le_u32(d));
                try!(writer.write_u8(h));
                try!(writer.write_u8(m));
                try!(writer.write_u8(s));
                try!(writer.write_le_u32(u));
            }
        };
        Ok(writer.unwrap())
    }
    pub fn from_payload(pld: &[u8], columns_count: uint) -> IoResult<Vec<Value>> {
        let mut output = Vec::with_capacity(columns_count);
        let mut reader = BufReader::new(pld);
        loop {
            if reader.eof() {
                break;
            } else if pld[try!(reader.tell()) as uint] == 0xfb {
                try!(reader.seek(1, SeekCur));
                output.push(NULL);
            } else {
                output.push(Bytes(try!(reader.read_lenenc_bytes())));
            }
        }
        Ok(output)
    }
    pub fn from_bin_payload(pld: &[u8], columns: &[Column]) -> IoResult<Vec<Value>> {
        let bit_offset = 2; // http://dev.mysql.com/doc/internals/en/null-bitmap.html
        let bitmap_len = (columns.len() + 7 + bit_offset) / 8;
        let mut bitmap = Vec::with_capacity(bitmap_len);
        let mut values = Vec::with_capacity(columns.len());
        for i in range(0, bitmap_len) {
            bitmap.push(pld[i+1]);
        }
        let mut reader = BufReader::new(pld[1 + bitmap_len..]);
        for i in range(0, columns.len()) {
            let c = &columns[i];
            if bitmap[(i + bit_offset) / 8] & (1 << ((i + bit_offset) % 8)) == 0 {
                values.push(try!(reader.read_bin_value(c.column_type,
                                                       c.flags.contains(consts::UNSIGNED_FLAG))));
            } else {
                values.push(NULL);
            }
        }
        Ok(values)
    }
    // (NULL-bitmap, values, ids of fields to send throwgh send_long_data)
    pub fn to_bin_payload(params: &[Column], values: &[Value], max_allowed_packet: uint) -> IoResult<(Vec<u8>, Vec<u8>, Option<Vec<u16>>)> {
        let bitmap_len = (params.len() + 7) / 8;
        let mut large_ids = Vec::new();
        let mut writer = MemWriter::new();
        let mut bitmap = Vec::from_elem(bitmap_len, 0u8);
        let mut i = 0u16;
        let mut written = 0;
        let cap = max_allowed_packet - bitmap_len - values.len() * 8;
        for value in values.iter() {
            match *value {
                NULL => bitmap[i as uint / 8] |= 1 << ((i % 8u16) as uint),
                _ => {
                    let val = try!(value.to_bin());
                    if val.len() < cap - written {
                        written += val.len();
                        try!(writer.write(val[]));
                    } else {
                        large_ids.push(i);
                    }
                }
            }
            i += 1;
        }
        if large_ids.len() == 0 {
            Ok((bitmap, writer.unwrap(), None))
        } else {
            Ok((bitmap, writer.unwrap(), Some(large_ids)))
        }
    }
}

pub trait ToValue {
    fn to_value(&self) -> Value;
}

impl<T:ToValue> ToValue for Option<T> {
    #[inline]
    fn to_value(&self) -> Value {
        match *self {
            None => NULL,
            Some(ref x) => x.to_value()
        }
    }
}

macro_rules! to_value_impl_num(
    ($t:ty) => (
        impl ToValue for $t {
            #[inline]
            fn to_value(&self) -> Value { Int(*self as i64) }
        }
    )
)

to_value_impl_num!(i8)
to_value_impl_num!(u8)
to_value_impl_num!(i16)
to_value_impl_num!(u16)
to_value_impl_num!(i32)
to_value_impl_num!(u32)
to_value_impl_num!(int)
to_value_impl_num!(i64)

impl ToValue for u64 {
    #[inline]
    fn to_value(&self) -> Value { UInt(*self) }
}

impl ToValue for uint {
    #[inline]
    fn to_value(&self) -> Value {
        let max: uint = Bounded::max_value();
        if *self as u64 <= max as u64 {
            Int(*self as i64)
        } else {
            UInt(*self as u64)
        }
    }
}

impl ToValue for f32 {
    #[inline]
    fn to_value(&self) -> Value { Float(*self as f64) }
}

impl ToValue for f64 {
    #[inline]
    fn to_value(&self) -> Value { Float(*self) }
}

impl ToValue for bool {
    #[inline]
    fn to_value(&self) -> Value { if *self { Int(1) } else { Int(0) }}
}

impl<'a> ToValue for &'a [u8] {
    #[inline]
    fn to_value(&self) -> Value { Bytes(self.to_vec()) }
}

impl ToValue for Vec<u8> {
    #[inline]
    fn to_value(&self) -> Value { Bytes(self.clone()) }
}

impl<'a> ToValue for &'a str {
    #[inline]
    fn to_value(&self) -> Value { Bytes(self.as_bytes().to_vec()) }
}

impl ToValue for String {
    #[inline]
    fn to_value(&self) -> Value { Bytes(self.as_bytes().to_vec()) }
}

impl ToValue for Value {
    #[inline]
    fn to_value(&self) -> Value { self.clone() }
}

impl ToValue for Timespec {
    #[inline]
    fn to_value(&self) -> Value {
        let t = at(*self);
        Date(t.tm_year as u16,
             (t.tm_mon + 1) as u8,
             t.tm_mday as u8,
             t.tm_hour as u8,
             t.tm_min as u8,
             t.tm_sec as u8,
             t.tm_nsec as u32 / 1000)
    }
}

impl ToValue for Duration {
    fn to_value(&self) -> Value {
        let mut this = self.clone();
        let neg = this.num_seconds() < 0;
        let mut days = this.num_days();
        if neg {
            days = 0 - days;
            this = this + Duration::days(days);
        } else {
            this = this - Duration::days(days);
        }
        let mut hrs = this.num_hours();
        if neg {
            hrs = 0 - hrs;
            this = this + Duration::hours(hrs);
        } else {
            this = this - Duration::hours(hrs);
        }
        let mut mins = this.num_minutes();
        if neg {
            mins = 0 - mins;
            this = this + Duration::minutes(mins);
        } else {
            this = this - Duration::minutes(mins);
        }
        let mut secs = this.num_seconds();
        if neg {
            secs = 0 - secs;
            this = this + Duration::seconds(secs);
        } else {
            this = this - Duration::seconds(secs);
        }
        let mut mics = this.num_microseconds().unwrap();
        if mics > 0 {
            if neg {
                mics = 1_000_000 - mics;
            }
        } else {
            mics = 0 - mics
        }
        Time(neg, days as u32, hrs as u8, mins as u8, secs as u8, mics as u32)
    }
}

pub trait FromValue {
    /// Will panic if could not retrieve `Self` from `Value`
    fn from_value(v: &Value) -> Self;

    /// Will return `None` if could not retrieve `Self` from `Value`
    fn from_value_opt(v: &Value) -> Option<Self>;
}

impl FromValue for Value {
    #[inline]
    fn from_value(v: &Value) -> Value { v.clone() }
    #[inline]
    fn from_value_opt(v: &Value) -> Option<Value> { Some(v.clone()) }
}

impl<T:FromValue> FromValue for Option<T> {
    #[inline]
    fn from_value(v: &Value) -> Option<T> {
        match *v {
            NULL => None,
            _ => Some(from_value(v))
        }
    }
    #[inline]
    fn from_value_opt(v: &Value) -> Option<Option<T>> {
        match *v {
            NULL => Some(None),
            _ => {
                match from_value_opt(v) {
                    None => None,
                    x => Some(x)
                }
            }
        }
    }
}

/// Will panic if could not retrieve `Self` from `Value`
#[inline]
pub fn from_value<T: FromValue>(v: &Value) -> T {
    FromValue::from_value(v)
}

/// Will return `None` if could not retrieve `Self` from `Value`
#[inline]
pub fn from_value_opt<T: FromValue>(v: &Value) -> Option<T> {
    FromValue::from_value_opt(v)
}

macro_rules! from_value_impl_num(
    ($t:ty) => (
        impl FromValue for $t {
            #[inline]
            fn from_value(v: &Value) -> $t {
                from_value_opt(v).expect("Error retrieving $t from value")
            }
            #[inline]
            fn from_value_opt(v: &Value) -> Option<$t> {
                let min: $t = Bounded::min_value();
                let max: $t = Bounded::max_value();
                match *v {
                    Int(x) if x >= min as i64 && x <= max as i64 => Some(x as $t),
                    UInt(x) if x <= max as u64 => Some(x as $t),
                    Bytes(ref bts) => {
                        from_utf8(bts[]).and_then(from_str::<$t>)
                    },
                    _ => None
                }
            }
        }
    )
)

from_value_impl_num!(i8)
from_value_impl_num!(u8)
from_value_impl_num!(i16)
from_value_impl_num!(u16)
from_value_impl_num!(i32)
from_value_impl_num!(u32)
from_value_impl_num!(int)
from_value_impl_num!(uint)

impl FromValue for i64 {
    #[inline]
    fn from_value(v: &Value) -> i64 {
        from_value_opt(v).expect("Error retrieving i64 from value")
    }
    #[inline]
    fn from_value_opt(v: &Value) -> Option<i64> {
        let max: i64 = Bounded::max_value();
        match *v {
            Int(x) => Some(x),
            UInt(x) if x <= max as u64 => Some(x as i64),
            Bytes(ref bts) => {
                from_utf8(bts[]).and_then(from_str::<i64>)
            },
            _ => None
        }
    }
}

impl FromValue for u64 {
    #[inline]
    fn from_value(v: &Value) -> u64 {
        from_value_opt(v).expect("Error retrieving u64 from value")
    }
    #[inline]
    fn from_value_opt(v: &Value) -> Option<u64> {
        match *v {
            Int(x) => Some(x as u64),
            UInt(x) => Some(x),
            Bytes(ref bts) => {
                from_utf8(bts[]).and_then(from_str::<u64>)
            },
            _ => None
        }
    }
}

impl FromValue for f32 {
    #[inline]
    fn from_value(v: &Value) -> f32 {
        from_value_opt(v).expect("Error retrieving f32 from value")
    }
    #[inline]
    fn from_value_opt(v: &Value) -> Option<f32> {
        let min: f32 = Bounded::min_value();
        let max: f32 = Bounded::max_value();
        match *v {
            Float(x) if x >= min as f64 && x <= max as f64 => Some(x as f32),
            Bytes(ref bts) => {
                from_utf8(bts[]).and_then(from_str::<f32>)
            },
            _ => None
        }
    }
}

impl FromValue for f64 {
    #[inline]
    fn from_value(v: &Value) -> f64 {
        from_value_opt(v).expect("Error retrieving f64 from value")
    }
    #[inline]
    fn from_value_opt(v: &Value) -> Option<f64> {
        match *v {
            Float(x) => Some(x),
            Bytes(ref bts) => {
                from_utf8(bts[]).and_then(from_str::<f64>)
            },
            _ => None
        }
    }
}

impl FromValue for bool {
    #[inline]
    fn from_value(v: &Value) -> bool {
        from_value_opt(v).expect("Error retrieving bool from value")
    }
    #[inline]
    fn from_value_opt(v:&Value) -> Option<bool> {
        match *v {
            Int(x) if x == 0 => Some(false),
            Int(x) if x == 1 => Some(true),
            Bytes(ref bts) if bts.len() == 1 && bts[0] == 0x30 => Some(false),
            Bytes(ref bts) if bts.len() == 1 && bts[0] == 0x31 => Some(true),
            _ => None
        }
    }
}

impl FromValue for Vec<u8> {
    #[inline]
    fn from_value(v: &Value) -> Vec<u8> {
        from_value_opt(v).expect("Error retrieving Vec<u8> from value")
    }
    #[inline]
    fn from_value_opt(v: &Value) -> Option<Vec<u8>> {
        match *v {
            Bytes(ref bts) => Some(bts.to_vec()),
            _ => None
        }
    }
}

impl FromValue for String {
    #[inline]
    fn from_value(v: &Value) -> String {
        from_value_opt(v).expect("Error retrieving String from value")
    }
    #[inline]
    fn from_value_opt(v: &Value) -> Option<String> {
        match *v {
            Bytes(ref bts) => {
                String::from_utf8(bts.clone()).ok()
            },
            _ => None
        }
    }
}

impl FromValue for Timespec {
    #[inline]
    fn from_value(v: &Value) -> Timespec {
        from_value_opt(v).expect("Error retrieving Timespec from value")
    }
    #[inline]
    fn from_value_opt(v: &Value) -> Option<Timespec> {
        match *v {
            Date(y, m, d, h, i, s, u) => {
                Some(Tm{
                        tm_year: y as i32 - 1_900,
                        tm_mon: m as i32 - 1,
                        tm_mday: d as i32,
                        tm_hour: h as i32,
                        tm_min: i as i32,
                        tm_sec: s as i32,
                        tm_nsec: u as i32 * 1_000,
                        tm_gmtoff: *TM_GMTOFF,
                        tm_wday: 0,
                        tm_yday: 0,
                        tm_isdst: *TM_ISDST,
                    }.to_timespec())
            },
            Bytes(ref bts) => {
                from_utf8(bts[]).and_then(|s| {
                    strptime(s, "%Y-%m-%d %H:%M:%S").or(strptime(s, "%Y-%m-%d")).ok()
                }).and_then(|mut tm| {
                    tm.tm_gmtoff = *TM_GMTOFF;
                    tm.tm_isdst = *TM_ISDST;
                    Some(tm.to_timespec())
                })
            },
            _ => None
        }
    }
}

impl FromValue for Duration {
    fn from_value(v: &Value) -> Duration {
        from_value_opt(v).expect("Error retrieving Duration from value")
    }

    fn from_value_opt(v: &Value) -> Option<Duration> {
        match *v {
            Time(neg, d, h, m, s, u) => {
                let microseconds = u as i64 + 
                    (s as i64 * 1_000_000) + 
                    (m as i64 * 60 * 1_000_000) +
                    (h as i64 * 60 * 60 * 1_000_000) +
                    (d as i64 * 24 * 60 * 60 * 1_000_000);
                if neg {
                    Some(Duration::microseconds(0 - microseconds))
                } else {
                    Some(Duration::microseconds(microseconds))
                }
            },
            Bytes(ref bts) => {
                let mut btss = bts[];
                let neg = btss[0] == b'-';
                if neg {
                    btss = bts[1..];
                }
                let ms: i64 = {
                    let xss: Vec<&[u8]> = btss.split(|x| *x == b'.').collect();
                    let ms: i64 = match xss[] {
                        [_, []] | [_] => 0,
                        [_, ms] if ms.len() <= 6 => {
                            let x = from_utf8(ms).and_then(from_str::<i64>);
                            if x.is_some() {
                                x.unwrap() * pow::<i64>(10,  6 - ms.len())
                            } else {
                                return None;
                            }
                        },
                        _ => {
                            return None;
                        }
                    };
                    if xss.len() == 2 {
                        btss = xss[0].clone();
                    }
                    ms
                };
                match btss {
                    // XXX:XX:XX
                    [h3@0x30 ... 0x38,
                     h2@0x30 ... 0x39,
                     h1@0x30 ... 0x39,
                     b':',
                     m2@0x30 ... 0x35,
                     m1@0x30 ... 0x39,
                     b':',
                     s2@0x30 ... 0x35,
                     s1@0x30 ... 0x39] => {
                        let s = (s2 as i64 & 0x0F) * 10 + (s1 as i64 & 0x0F);
                        let m = (m2 as i64 & 0x0F) * 10 + (m1 as i64 & 0x0F);
                        let h = (h3 as i64 & 0x0F) * 100 +
                                (h2 as i64 & 0x0F) * 10 +
                                (h1 as i64 & 0x0F);
                        let microseconds = ms +
                                           s * 1_000_000 +
                                           m * 60 * 1_000_000 +
                                           h * 60 * 60 * 1_000_000;
                        if neg {
                            Some(Duration::microseconds(0 - microseconds))
                        } else {
                            Some(Duration::microseconds(microseconds))
                        }
                    },
                    // XX:XX:XX
                    [h2@0x30 ... 0x39,
                     h1@0x30 ... 0x39,
                     b':',
                     m2@0x30 ... 0x35,
                     m1@0x30 ... 0x39,
                     b':',
                     s2@0x30 ... 0x35,
                     s1@0x30 ... 0x39] => {
                        let s = (s2 as i64 | 0x0F) * 10 + (s1 as i64 | 0x0F);
                        let m = (m2 as i64 | 0x0F) * 10 + (m1 as i64 | 0x0F);
                        let h = (h2 as i64 | 0x0F) * 10 +
                                (h1 as i64 | 0x0F);
                        let microseconds = ms +
                                           s * 1_000_000 +
                                           m * 60 * 1_000_000 +
                                           h * 60 * 60 * 1_000_000;
                        if neg {
                            Some(Duration::microseconds(0 - microseconds))
                        } else {
                            Some(Duration::microseconds(microseconds))
                        }
                    },
                    _ => None
                }
            },
            _ => None
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Bytes, Int, UInt, Date, Time, Float, NULL, from_value, ToValue};
    use time::{Timespec, now};
    use std::time::{Duration};

    #[test]
    fn test_value_into_str() {
        let v = NULL;
        assert_eq!(v.into_str(), "NULL".to_string());
        let v = Bytes(b"hello".to_vec());
        assert_eq!(v.into_str(), "'hello'".to_string());
        let v = Bytes(b"h\x5c'e'l'l'o".to_vec());
        assert_eq!(v.into_str(), "'h\x5c\x5c\x5c'e\x5c'l\x5c'l\x5c'o'".to_string());
        let v = Bytes(vec!(0, 1, 2, 3, 4, 255));
        assert_eq!(v.into_str(), "0x0001020304FF".to_string());
        let v = Int(-65536);
        assert_eq!(v.into_str(), "-65536".to_string());
        let v = UInt(4294967296);
        assert_eq!(v.into_str(), "4294967296".to_string());
        let v = Float(686.868);
        assert_eq!(v.into_str(), "686.868".to_string());
        let v = Date(0, 0, 0, 0, 0, 0, 0);
        assert_eq!(v.into_str(), "''".to_string());
        let v = Date(2014, 2, 20, 0, 0, 0, 0);
        assert_eq!(v.into_str(), "'2014-02-20'".to_string());
        let v = Date(2014, 2, 20, 22, 0, 0, 0);
        assert_eq!(v.into_str(), "'2014-02-20 22:00:00'".to_string());
        let v = Date(2014, 2, 20, 22, 0, 0, 1);
        assert_eq!(v.into_str(), "'2014-02-20 22:00:00.000001'".to_string());
        let v = Time(false, 0, 0, 0, 0, 0);
        assert_eq!(v.into_str(), "''".to_string());
        let v = Time(true, 34, 3, 2, 1, 0);
        assert_eq!(v.into_str(), "'-819:02:01'".to_string());
        let v = Time(false, 10, 100, 20, 30, 40);
        assert_eq!(v.into_str(), "'340:20:30.000040'".to_string());
    }

    #[test]
    fn test_from_value() {
        assert_eq!(-100i8, from_value::<i8>(&Int(-100i64)));
        assert_eq!(100i8, from_value::<i8>(&UInt(100u64)));
        assert_eq!(100i8, from_value::<i8>(&Bytes(b"100".to_vec())));
        assert_eq!(Some(100i8),
                   from_value::<Option<i8>>(&Bytes(b"100".to_vec())));
        assert_eq!(None, from_value::<Option<i8>>(&NULL));
        assert_eq!(b"test".to_vec(),
                   from_value::<Vec<u8>>(&Bytes(b"test".to_vec())));
        assert_eq!("test".to_string(),
                   from_value::<String>(&Bytes(b"test".to_vec())));
        assert_eq!(true, from_value::<bool>(&Int(1)));
        assert_eq!(false, from_value::<bool>(&Int(0)));
        assert_eq!(true, from_value::<bool>(&Bytes(b"1".to_vec())));
        assert_eq!(false, from_value::<bool>(&Bytes(b"0".to_vec())));
        assert_eq!(Timespec {sec: 1414866780 - now().tm_gmtoff as i64, nsec: 0},
                   from_value::<Timespec>(&Bytes(b"2014-11-01 18:33:00".to_vec())));
        assert_eq!(Timespec {sec: 1414866780 - now().tm_gmtoff as i64, nsec: 1000},
                   from_value::<Timespec>(&Date(2014, 11, 1, 18, 33, 00, 1)));
        assert_eq!(Timespec {sec: 1414800000 - now().tm_gmtoff as i64, nsec: 0},
                   from_value::<Timespec>(&Bytes(b"2014-11-01".to_vec())));
        assert_eq!(Duration::milliseconds(-433830500),
                   from_value::<Duration>(&Bytes(b"-120:30:30.5".to_vec())));
    }

    #[test]
    fn test_to_value() {
        assert_eq!(Duration::milliseconds(-433830500).to_value(),
                   Time(true, 5, 0, 30, 30, 500000));
    }

    #[test]
    #[should_fail]
    fn test_from_value_panic_i8_1() {
        from_value::<i8>(&Int(500i64));
    }

    #[test]
    #[should_fail]
    fn test_from_value_panic_i8_2() {
        from_value::<i8>(&Bytes(b"500".to_vec()));
    }

    #[test]
    #[should_fail]
    #[allow(non_snake_case)]
    fn test_from_value_panic_Timespec() {
        from_value::<Timespec>(&Bytes(b"2014-50-01".to_vec()));
    }
}
