use std::str::FromStr;
use std::str::from_utf8;
use std::borrow::ToOwned;
// XXX: Wait for Duration stabilization
use std::io;
use std::io::Write as stdWrite;
use time::{Tm, Timespec, now, strptime, at};

use super::consts;
use super::conn::{Column};
use super::io::{Write, Read};

use byteorder::LittleEndian as LE;
use byteorder::{ByteOrder, WriteBytesExt};

lazy_static! {
    static ref TM_UTCOFF: i32 = now().tm_utcoff;
    static ref TM_ISDST: i32 = now().tm_isdst;
}

/// `Value` enumerates possible values in mysql cells. Also `Value` used to fill
/// prepared statements. Note that to receive something other than `Value::NULL`
/// or `Value::Bytes` from mysql you should use prepared statements.
///
/// If you want to get something more useful from `Value` you should implement
/// [`FromValue`](trait.FromValue.html) on it. To get `T: FromValue` from
/// nullable value you should rely on `FromValue` implemented on `Option<T>`.
///
/// To convert something to `Value` you should implement
/// [`ToValue`](trait.ToValue.html) on it.
///
/// ```rust
/// # use mysql::conn::pool;
/// # use mysql::conn::MyOpts;
/// use mysql::value::from_value;
/// # use std::thread::Thread;
/// # use std::default::Default;
/// # fn get_opts() -> MyOpts {
/// #     MyOpts {
/// #         user: Some("root".to_string()),
/// #         pass: Some("password".to_string()),
/// #         tcp_addr: Some("127.0.0.1".to_string()),
/// #         tcp_port: 3307,
/// #         ..Default::default()
/// #     }
/// # }
/// # let opts = get_opts();
/// # let pool = pool::MyPool::new(opts).unwrap();
/// let mut conn = pool.get_conn().unwrap();
///
/// conn.prepare("SELECT ? * ?").map(|mut stmt| {
///     let mut result = stmt.execute((20i32, 0.8f32)).unwrap();
///     for row in result {
///         let mut row = row.unwrap();
///         assert_eq!(from_value::<f32>(row.pop().unwrap()), 16.0f32);
///     }
/// });
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

impl IntoValue for Value {
    fn into_value(self) -> Value {
        self
    }
}


pub trait FromValue {
    /// Will panic if could not retrieve `Self` from `Value`
    fn from_value(v: Value) -> Self;

    /// Will return `None` if coluld not retrieve `Self` from `Value`
    fn from_value_opt(v: Value) -> Option<Self>;
}

impl FromValue for Value {
    fn from_value(v: Value) -> Value {
        v
    }
    fn from_value_opt(v: Value) -> Option<Value> {
        Some(v)
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(v: Value) -> Option<T> {
        match v {
            Value::NULL => None,
            v => Some(FromValue::from_value(v))
        }
    }
    fn from_value_opt(v: Value) -> Option<Option<T>> {
        match v {
            Value::NULL => Some(None),
            v => {
                match FromValue::from_value_opt(v) {
                    None => None,
                    x => Some(x),
                }
            }
        }
    }
}

/// Will panic if could not retrieve `Self` from `Value`
#[inline]
pub fn from_value<T: FromValue>(v: Value) -> T {
    FromValue::from_value(v)
}

/// Will return `None` if could not retrieve `Self` from `Value`
#[inline]
pub fn from_value_opt<T: FromValue>(v: Value) -> Option<T> {
    FromValue::from_value_opt(v)
}

macro_rules! from_value_impl_num {
    ($t:ident) => (
        impl FromValue for $t {
            fn from_value(v: Value) -> $t {
                from_value_opt(v).expect("Error retrieving from value")
            }
            fn from_value_opt(v: Value) -> Option<$t> {
                match v {
                    Value::Int(x) => {
                        let min = ::std::$t::MIN as i64;
                        let mut max = ::std::$t::MAX as i64;
                        if max < 0 {
                            max = ::std::i64::MAX;
                        }
                        if min <= x && x <= max {
                            Some(x as $t)
                        } else {
                            None
                        }
                    },
                    Value::UInt(x) if x <= ::std::$t::MAX as u64 => Some(x as $t),
                    Value::Bytes(ref bts) => {
                        from_utf8(&bts[..]).ok().and_then(|x| {
                            FromStr::from_str(x).ok()
                        })
                    },
                    _ => None,
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
        from_value_opt(v).expect("Error retrieving i64 from value")
    }
    fn from_value_opt(v: Value) -> Option<i64> {
        match v {
            Value::Int(x) => Some(x),
            Value::UInt(x) if x <= ::std::i64::MAX as u64 => Some(x as i64),
            Value::Bytes(bts) => {
                from_utf8(&bts[..]).ok().and_then(|x| {
                    FromStr::from_str(x).ok()
                })
            },
            _ => None
        }
    }
}

impl FromValue for u64 {
    fn from_value(v: Value) -> u64 {
        from_value_opt(v).expect("Error retrieving u64 from value")
    }
    fn from_value_opt(v: Value) -> Option<u64> {
        match v {
            Value::Int(x) if x >= 0 => Some(x as u64),
            Value::UInt(x) => Some(x),
            Value::Bytes(bts) => {
                from_utf8(&bts[..]).ok().and_then(|x| {
                    FromStr::from_str(x).ok()
                })
            },
            _ => None
        }
    }
}

impl FromValue for f32 {
    fn from_value(v: Value) -> f32 {
        from_value_opt(v).expect("Error retrieving f32 from value")
    }
    fn from_value_opt(v: Value) -> Option<f32> {
        match v {
            Value::Float(x) if x >= ::std::f32::MIN as f64 && x <= ::std::f32::MAX as f64 => Some(x as f32),
            Value::Bytes(bts) => {
                from_utf8(&bts[..]).ok().and_then(|x| {
                    FromStr::from_str(x).ok()
                })
            },
            _ => None
        }
    }
}

impl FromValue for f64 {
    fn from_value(v: Value) -> f64 {
        from_value_opt(v).expect("Error retrieving f64 from value")
    }
    #[inline]
    fn from_value_opt(v: Value) -> Option<f64> {
        match v {
            Value::Float(x) => Some(x),
            Value::Bytes(bts) => {
                from_utf8(&bts[..]).ok().and_then(|x| {
                    FromStr::from_str(x).ok()
                })
            },
            _ => None
        }
    }
}

impl FromValue for bool {
    fn from_value(v: Value) -> bool {
        from_value_opt(v).expect("Error retrieving bool from value")
    }
    fn from_value_opt(v: Value) -> Option<bool> {
        match v {
            Value::Int(0) => Some(false),
            Value::Int(1) => Some(true),
            Value::Bytes(ref bts) if bts.len() == 1 && bts[0] == 0x30 => Some(false),
            Value::Bytes(ref bts) if bts.len() == 1 && bts[0] == 0x31 => Some(true),
            _ => None
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_value(v: Value) -> Vec<u8> {
        from_value_opt(v).expect("Error retrieving Vec<u8> from value")
    }
    fn from_value_opt(v: Value) -> Option<Vec<u8>> {
        match v {
            Value::Bytes(bts) => Some(bts),
            _ => None
        }
    }
}

impl FromValue for String {
    fn from_value(v: Value) -> String {
        from_value_opt(v).expect("Error retrieving String from value")
    }
    fn from_value_opt(v: Value) -> Option<String> {
        match v {
            Value::Bytes(bts) => {
                String::from_utf8(bts).ok()
            },
            _ => None
        }
    }
}

impl FromValue for Timespec {
    fn from_value(v: Value) -> Timespec {
        from_value_opt(v).expect("Error retrieving Timespec from Value")
    }
    fn from_value_opt(v: Value) -> Option<Timespec> {
        match v {
            Value::Date(y, m, d, h, i, s, u) => {
                Some(Tm{
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
            Value::Bytes(bts) => {
                from_utf8(&bts[..]).ok().and_then(|s| {
                    strptime(s, "%Y-%m-%d %H:%M:%S").or(strptime(s, "%Y-%m-%d")).ok()
                }).and_then(|mut tm| {
                    tm.tm_utcoff = *TM_UTCOFF;
                    tm.tm_isdst = *TM_ISDST;
                    Some(tm.to_timespec())
                })
            },
            _ => None
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
        use super::super::from_value;
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
        use super::super::IntoValue;
        use super::super::Value::{Date};
        use time::{Timespec, now};
        #[test]
        fn should_convert_Time_to_Date() {
            let ts = Timespec {
                sec: 1414866780 - now().tm_utcoff as i64,
                nsec: 1000,
            };
            assert_eq!(ts.into_value(), Date(2014, 11, 1, 18, 33, 00, 1));
        }
    }
}
