use std::io::{MemWriter, BufReader, IoResult, SeekCur};
use std::from_str::{from_str};
use std::str::{from_utf8};
use std::num::{Bounded};
use time::{Tm, Timespec, now, strptime};
use super::consts::{UNSIGNED_FLAG};
use super::conn::{Column};
use super::io::{MyWriter, MyReader};

lazy_static! {
    static ref TM_GMTOFF: i32 = now().tm_gmtoff;
    static ref TM_ISDST: i32 = now().tm_isdst;
    static ref MIN_i8: i8 = Bounded::min_value();
    static ref MAX_i8: i8 = Bounded::max_value();
    static ref MIN_i16: i16 = Bounded::min_value();
    static ref MAX_i16: i16 = Bounded::max_value();
    static ref MIN_i32: i32 = Bounded::min_value();
    static ref MAX_i32: i32 = Bounded::max_value();
    static ref MIN_u8: u8 = Bounded::min_value();
    static ref MAX_u8: u8 = Bounded::max_value();
    static ref MIN_u16: u16 = Bounded::min_value();
    static ref MAX_u16: u16 = Bounded::max_value();
    static ref MIN_u32: u32 = Bounded::min_value();
    static ref MAX_u32: u32 = Bounded::max_value();
    static ref MIN_int: int = Bounded::min_value();
    static ref MAX_int: int = Bounded::max_value();
    static ref MIN_uint: uint = Bounded::min_value();
    static ref MAX_uint: uint = Bounded::max_value();
    static ref MAX_i64: i64 = Bounded::max_value();
    static ref MIN_f32: f32 = Bounded::min_value();
    static ref MAX_f32: f32 = Bounded::max_value();
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
            NULL => String::from_str("NULL"),
            Bytes(ref x) => {
                match String::from_utf8(x.clone()) {
                    Ok(s) => {
                        let replaced = s.replace("\x5c", "\x5c\x5c")
                                        .replace("\x00", "\x5c\x00")
                                        .replace("\n", "\x5c\n")
                                        .replace("\r", "\x5c\r")
                                        .replace("'", "\x5c'")
                                        .replace("\"", "\x5c\"")
                                        .replace("\x1a", "\x5c\x1a");
                        format!("'{:s}'", replaced)
                    },
                    Err(_) => {
                        let mut s = String::from_str("0x");
                        for c in x.iter() {
                            s = s.append(format!("{:02X}", *c).as_slice());
                        }
                        s
                    }
                }
            },
            Int(x) => format!("{:d}", x),
            UInt(x) => format!("{:u}", x),
            Float(x) => format!("{:f}", x),
            Date(0, 0, 0, 0, 0, 0, 0) => "''".to_string(),
            Date(y, m, d, 0, 0, 0, 0) => format!("'{:04u}-{:02u}-{:02u}'", y, m, d),
            Date(y, m, d, h, i, s, 0) => format!("'{:04u}-{:02u}-{:02u} {:02u}:{:02u}:{:02u}'", y, m, d, h, i, s),
            Date(y, m, d, h, i, s, u) => format!("'{:04u}-{:02u}-{:02u} {:02u}:{:02u}:{:02u}.{:06u}'", y, m, d, h, i, s, u),
            Time(_, 0, 0, 0, 0, 0) => "''".to_string(),
            Time(neg, d, h, i, s, 0) => {
                if neg {
                    format!("'-{:u} {:03u}:{:02u}:{:02u}'", d, h, i, s)
                } else {
                    format!("'{:u} {:03u}:{:02u}:{:02u}'", d, h, i, s)
                }
            },
            Time(neg, d, h, i, s, u) => {
                if neg {
                    format!("'-{:u} {:03u}:{:02u}:{:02u}.{:06u}'", d, h, i, s, u)
                } else {
                    format!("'{:u} {:03u}:{:02u}:{:02u}.{:06u}'", d, h, i, s, u)
                }
            }
        }
    }
    pub fn is_bytes(&self) -> bool {
        match *self {
            Bytes(..) => true,
            _ => false
        }
    }
    pub fn bytes_ref<'a>(&'a self) -> &'a [u8] {
        match *self {
            Bytes(ref x) => x.as_slice(),
            _ => fail!("Called `Value::bytes_ref()` on non `Bytes` value")
        }
    }
    pub fn unwrap_bytes(self) -> Vec<u8> {
        match self {
            Bytes(x) => x,
            _ => fail!("Called `Value::unwrap_bytes()` on non `Bytes` value")
        }
    }
    pub fn unwrap_bytes_or(self, y: Vec<u8>) -> Vec<u8> {
        match self {
            Bytes(x) => x,
            _ => y
        }
    }
    pub fn is_int(&self) -> bool {
        match *self {
            Int(..) => true,
            _ => false
        }
    }
    pub fn get_int(&self) -> i64 {
        match *self {
            Int(x) => x,
            _ => fail!("Called `Value::get_int()` on non `Int` value")
        }
    }
    pub fn get_int_or(&self, y: i64) -> i64 {
        match *self {
            Int(x) => x,
            _ => y
        }
    }
    pub fn is_uint(&self) -> bool {
        match *self {
            UInt(..) => true,
            _ => false
        }
    }
    pub fn get_uint(&self) -> u64 {
        match *self {
            UInt(x) => x,
            _ => fail!("Called `Value::get_uint()` on non `UInt` value")
        }
    }
    pub fn get_uint_or(&self, y: u64) -> u64 {
        match *self {
            UInt(x) => x,
            _ => y
        }
    }
    pub fn is_float(&self) -> bool {
        match *self {
            Float(..) => true,
            _ => false
        }
    }
    pub fn get_float(&self) -> f64 {
        match *self {
            Float(x) => x,
            _ => fail!("Called `Value::get_float()` on non `Float` value")
        }
    }
    pub fn get_float_or(&self, y: f64) -> f64 {
        match *self {
            Float(x) => x,
            _ => y
        }
    }
    pub fn is_date(&self) -> bool {
        match *self {
            Date(..) => true,
            _ => false
        }
    }
    pub fn get_year(&self) -> u16 {
        match *self {
            Date(y, _, _, _, _, _, _) => y,
            _ => fail!("Called `Value::get_year()` on non `Date` value")
        }
    }
    pub fn get_month(&self) -> u8 {
        match *self {
            Date(_, m, _, _, _, _, _) => m,
            _ => fail!("Called `Value::get_month()` on non `Date` value")
        }
    }
    pub fn get_day(&self) -> u8 {
        match *self {
            Date(_, _, d, _, _, _, _) => d,
            _ => fail!("Called `Value::get_day()` on non `Date` value")
        }
    }
    pub fn is_time(&self) -> bool {
        match *self {
            Time(..) => true,
            _ => false
        }
    }
    pub fn is_neg(&self) -> bool {
        match *self {
            Time(false, _, _, _, _, _) => false,
            Time(true, _, _, _, _, _) => true,
            _ => fail!("Called `Value::is_neg()` on non `Time` value")
        }
    }
    pub fn get_days(&self) -> u32 {
        match *self {
            Time(_, d, _, _, _, _) => d,
            _ => fail!("Called `Value::get_days()` on non `Time` value")
        }
    }
    pub fn get_hour(&self) -> u8 {
        match *self {
            Date(_, _, _, h, _, _, _) => h,
            Time(_, _, h, _, _, _) => h,
            _ => fail!("Called `Value::get_hour()` on non `Date` nor `Time` value")
        }
    }
    pub fn get_min(&self) -> u8 {
        match *self {
            Date(_, _, _, _, i, _, _) => i,
            Time(_, _, _, i, _, _) => i,
            _ => fail!("Called `Value::get_min()` on non `Date` nor `Time` value")
        }
    }
    pub fn get_sec(&self) -> u8 {
        match *self {
            Date(_, _, _, _, _, s, _) => s,
            Time(_, _, _, _, s, _) => s,
            _ => fail!("Called `Value::get_sec()` on non `Date` nor `Time` value")
        }
    }
    pub fn get_usec(&self) -> u32 {
        match *self {
            Date(_, _, _, _, _, _, u) => u,
            Time(_, _, _, _, _, u) => u,
            _ => fail!("Called `Value::get_usec()` on non `Date` nor `Time` value")
        }
    }
    pub fn to_bin(&self) -> IoResult<Vec<u8>> {
        let mut writer = MemWriter::with_capacity(256);
        match *self {
            NULL => (),
            Bytes(ref x) => {
                try!(writer.write_lenenc_bytes(x.as_slice()));
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
        let mut output: Vec<Value> = Vec::with_capacity(columns_count);
        let mut reader = BufReader::new(pld);
        loop {
            if reader.eof() {
                break;
            } else if { let pos = try!(reader.tell()); pld[pos as uint] == 0xfb_u8 } {
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
        let mut bitmap: Vec<u8> = Vec::with_capacity(bitmap_len);
        let mut values: Vec<Value> = Vec::with_capacity(columns.len());
        let mut i = -1;
        while {i += 1; i < bitmap_len} {
            bitmap.push(pld[i+1]);
        }
        let mut reader = BufReader::new(pld.slice_from(1 + bitmap_len));
        let mut i = -1;
        while {i += 1; i < columns.len()} {
            if *bitmap.get((i + bit_offset) / 8) & (1 << ((i + bit_offset) % 8)) == 0 {
                values.push(try!(reader.read_bin_value(columns[i].column_type,
                                                       (columns[i].flags & UNSIGNED_FLAG as u16) != 0)));
            } else {
                values.push(NULL);
            }
        }
        Ok(values)
    }
    // (NULL-bitmap, values, ids of fields to send throwgh send_long_data)
    pub fn to_bin_payload(params: &[Column], values: &[Value], max_allowed_packet: uint) -> IoResult<(Vec<u8>, Vec<u8>, Option<Vec<u16>>)> {
        let bitmap_len = (params.len() + 7) / 8;
        let mut large_ids: Vec<u16> = Vec::new();
        let mut writer = MemWriter::new();
        let mut bitmap = Vec::from_elem(bitmap_len, 0u8);
        let mut i = 0u16;
        let mut written = 0;
        let cap = max_allowed_packet - bitmap_len - values.len() * 8;
        for value in values.iter() {
            match *value {
                NULL => *bitmap.get_mut(i as uint / 8) |= 1 << ((i % 8u16) as uint),
                _ => {
                    let val = try!(value.to_bin());
                    if val.len() < cap - written {
                        written += val.len();
                        try!(writer.write(val.as_slice()));
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

pub trait FromValue {
    fn from_value(v: &Value) -> Self;
}

pub fn from_value<T: FromValue>(v: &Value) -> T {
    FromValue::from_value(v)
}

macro_rules! from_value_impl_opt(
    ($t:ty) => (
        impl FromValue for Option<$t> {
            fn from_value(v: &Value) -> Option<$t> {
                match *v {
                    NULL => None,
                    _ => Some(from_value(v))
                }
            }
        }
    )
)

macro_rules! from_value_impl_num(
    ($t:ty, $min:ident, $max:ident) => (
        impl FromValue for $t {
            fn from_value(v: &Value) -> $t {
                match *v {
                    Int(x) if x >= *$min as i64 && x <= *$max as i64 => x as $t,
                    UInt(x) if x <= *$max as u64 => x as $t,
                    Bytes(ref bts) => {
                        from_utf8(bts.as_slice()).and_then(|s| {
                            from_str::<$t>(s)
                        }).expect("Error retrieving $t from value")
                    },
                    _ => fail!("Error retrieving $t from value")
                }
            }
        }

        from_value_impl_opt!($t)
    )
)

from_value_impl_num!(i8, MIN_i8, MAX_i8)
from_value_impl_num!(u8, MIN_u8, MAX_u8)
from_value_impl_num!(i16, MIN_i16, MAX_i16)
from_value_impl_num!(u16, MIN_u16, MAX_u16)
from_value_impl_num!(i32, MIN_i32, MAX_i32)
from_value_impl_num!(u32, MIN_u32, MAX_u32)
from_value_impl_num!(int, MIN_int, MAX_int)
from_value_impl_num!(uint, MIN_uint, MAX_uint)

impl FromValue for i64 {
    fn from_value(v: &Value) -> i64 {
        match *v {
            Int(x) => x,
            UInt(x) if x <= *MAX_i64 as u64 => x as i64,
            Bytes(ref bts) => {
                from_utf8(bts.as_slice()).and_then(|s| {
                    from_str::<i64>(s)
                }).expect("Error retrieving i64 from value")
            },
            _ => fail!("Error retrieving i64 from value")
        }
    }
}
from_value_impl_opt!(i64)

impl FromValue for u64 {
    fn from_value(v: &Value) -> u64 {
        match *v {
            Int(x) => x as u64,
            UInt(x) => x,
            Bytes(ref bts) => {
                from_utf8(bts.as_slice()).and_then(|s| {
                    from_str::<u64>(s)
                }).expect("Error retrieving u64 from value")
            },
            _ => fail!("Error retrieving u64 from value")
        }
    }
}
from_value_impl_opt!(u64)

impl FromValue for f32 {
    fn from_value(v: &Value) -> f32 {
        match *v {
            Float(x) if x >= *MIN_f32 as f64 && x <= *MAX_f32 as f64 => x as f32,
            Bytes(ref bts) => {
                from_utf8(bts.as_slice()).and_then(|s| {
                    from_str::<f32>(s)
                }).expect("Error retrieving f32 from value")
            },
            _ => fail!("Error retrieving f32 from value")
        }
    }
}
from_value_impl_opt!(f32)

impl FromValue for f64 {
    fn from_value(v: &Value) -> f64 {
        match *v {
            Float(x) => x,
            Bytes(ref bts) => {
                from_utf8(bts.as_slice()).and_then(|s| {
                    from_str::<f64>(s)
                }).expect("Error retrieving f64 from value")
            },
            _ => fail!("Error retrieving f64 from value")
        }
    }
}
from_value_impl_opt!(f64)

impl FromValue for bool {
    fn from_value(v:&Value) -> bool {
        match *v {
            Int(x) if x == 0 => false,
            Int(x) if x == 1 => true,
            Bytes(ref bts) if bts.len() == 1 && *bts.get(0) == 0x30 => false,
            Bytes(ref bts) if bts.len() == 1 && *bts.get(0) == 0x31 => true,
            _ => fail!("Error retrieving bool from value")
        }
    }
}
from_value_impl_opt!(bool)

impl FromValue for Vec<u8> {
    fn from_value(v: &Value) -> Vec<u8> {
        match *v {
            Bytes(ref bts) => Vec::from_slice(bts.as_slice()),
            _ => fail!("Error retrieving Vec<u8> from value")
        }
    }
}
from_value_impl_opt!(Vec<u8>)

impl FromValue for String {
    fn from_value(v: &Value) -> String {
        match *v {
            Bytes(ref bts) => {
                from_utf8(bts.as_slice()).and_then(|s| {
                    Some(String::from_str(s))
                }).expect("Error retrieving String from value")
            },
            _ => fail!("Error retrieving String from value")
        }
    }
}
from_value_impl_opt!(String)

impl FromValue for Timespec {
    fn from_value(v: &Value) -> Timespec {
        match *v {
            Date(y, m, d, h, i, s, u) => {
                Tm{
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
                }.to_timespec()
            },
            Bytes(ref bts) => {
                from_utf8(bts.as_slice()).and_then(|s| {
                    strptime(s, "%Y-%m-%d %H:%M:%S").or(strptime(s, "%Y-%m-%d")).ok()
                }).and_then(|mut tm| {
                    tm.tm_gmtoff = *TM_GMTOFF;
                    tm.tm_isdst = *TM_ISDST;
                    Some(tm)
                }).expect("Error retrieving Timespec from value").to_timespec()
            },
            _ => fail!("Error retrieving Timespec from value")
        }
    }
}
from_value_impl_opt!(Timespec)

#[cfg(test)]
mod test {
    use super::{Bytes, Int, UInt, Date, Time, Float, NULL, from_value};
    use time::{Timespec};

    #[test]
    fn test_value_into_str() {
        let v = NULL;
        assert_eq!(v.into_str(), "NULL".to_string());
        let v = Bytes(Vec::from_slice(b"hello"));
        assert_eq!(v.into_str(), "'hello'".to_string());
        let v = Bytes(Vec::from_slice(b"h\x5c'e'l'l'o"));
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
        assert_eq!(v.into_str(), "'-34 003:02:01'".to_string());
        let v = Time(false, 10, 100, 20, 30, 40);
        assert_eq!(v.into_str(), "'10 100:20:30.000040'".to_string());
    }

    #[test]
    fn test_from_value() {
        assert_eq!(-100i8, from_value::<i8>(&Int(-100i64)));
        assert_eq!(100i8, from_value::<i8>(&UInt(100u64)));
        assert_eq!(100i8, from_value::<i8>(&Bytes(Vec::from_slice(b"100"))));
        assert_eq!(Some(100i8), from_value::<Option<i8>>(&Bytes(Vec::from_slice(b"100"))));
        assert_eq!(None, from_value::<Option<i8>>(&NULL));
        assert_eq!(Vec::from_slice(b"test"), from_value::<Vec<u8>>(&Bytes(Vec::from_slice(b"test"))));
        assert_eq!("test".to_string(), from_value::<String>(&Bytes(Vec::from_slice(b"test"))));
        assert_eq!(true, from_value::<bool>(&Int(1)));
        assert_eq!(false, from_value::<bool>(&Int(0)));
        assert_eq!(true, from_value::<bool>(&Bytes(Vec::from_slice(b"1"))));
        assert_eq!(false, from_value::<bool>(&Bytes(Vec::from_slice(b"0"))));
        assert_eq!(Timespec{sec: 1404241033, nsec: 0},
                   from_value::<Timespec>(&Bytes(Vec::from_slice(b"2014-07-01 22:57:13"))));
        assert_eq!(Timespec{sec: 1404241033, nsec: 1000},
                   from_value::<Timespec>(&Date(2014, 7, 1, 22, 57, 13, 1)));
        assert_eq!(Timespec{sec: 1404158400, nsec: 0},
                   from_value::<Timespec>(&Bytes(Vec::from_slice(b"2014-07-01"))));
    }

    #[test]
    #[should_fail]
    fn test_from_value_fail_i8_1() {
        from_value::<i8>(&Int(500i64));
    }

    #[test]
    #[should_fail]
    fn test_from_value_fail_i8_2() {
        from_value::<i8>(&Bytes(Vec::from_slice(b"500")));
    }

    #[test]
    #[should_fail]
    #[allow(non_snake_case_functions)]
    fn test_from_value_fail_Timespec() {
        from_value::<Timespec>(&Bytes(Vec::from_slice(b"2014-50-01")));
    }
}
