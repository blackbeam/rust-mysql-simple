extern mod extra;
use std::io::{Decorator, Stream, Reader};
use std::io::mem::{BufReader, MemWriter};
use std::io::net::ip::{SocketAddr, Ipv4Addr};
use std::io::net::tcp::{TcpStream};
use std::io::net::unix::{UnixStream};
use std::vec;
use consts;
use scramble::{scramble};
use std::fmt;
use std::str;
use std::uint;
use std::io::File;

/***
 *     .d88888b.  888      8888888b.                    888               888    
 *    d88P" "Y88b 888      888   Y88b                   888               888    
 *    888     888 888      888    888                   888               888    
 *    888     888 888  888 888   d88P  8888b.   .d8888b 888  888  .d88b.  888888 
 *    888     888 888 .88P 8888888P"      "88b d88P"    888 .88P d8P  Y8b 888    
 *    888     888 888888K  888        .d888888 888      888888K  88888888 888    
 *    Y88b. .d88P 888 "88b 888        888  888 Y88b.    888 "88b Y8b.     Y88b.  
 *     "Y88888P"  888  888 888        "Y888888  "Y8888P 888  888  "Y8888   "Y888 
 *                                                                               
 *                                                                               
 *                                                                               
 */

struct OkPacket {
    affected_rows: u64,
    last_insert_id: u64,
    status_flags: u16,
    warnings: u16,
    info: ~[u8]
}

impl OkPacket {
    #[inline]
    fn from_payload(pld: &[u8]) -> OkPacket {
        let mut reader = BufReader::new(pld);
        let _ = reader.read_byte();
        OkPacket{
            affected_rows: reader.read_lenenc_int(),
            last_insert_id: reader.read_lenenc_int(),
            status_flags: reader.read_le_u16(),
            warnings: reader.read_le_u16(),
            info: reader.read_to_end()
        }
    }
}

/***
 *    8888888888                 8888888b.                    888               888    
 *    888                        888   Y88b                   888               888    
 *    888                        888    888                   888               888    
 *    8888888    888d888 888d888 888   d88P  8888b.   .d8888b 888  888  .d88b.  888888 
 *    888        888P"   888P"   8888888P"      "88b d88P"    888 .88P d8P  Y8b 888    
 *    888        888     888     888        .d888888 888      888888K  88888888 888    
 *    888        888     888     888        888  888 Y88b.    888 "88b Y8b.     Y88b.  
 *    8888888888 888     888     888        "Y888888  "Y8888P 888  888  "Y8888   "Y888 
 *                                                                                     
 *                                                                                     
 *                                                                                     
 */

struct ErrPacket {
    sql_state: ~[u8],
    error_message: ~[u8],
    error_code: u16
}

impl ErrPacket {
    #[inline]
    fn from_payload(pld: &[u8]) -> ErrPacket {
        let mut reader = BufReader::new(pld);
        let _ = reader.read_byte();
        let error_code = reader.read_le_u16();
        let _ = reader.read_byte();
        ErrPacket{
            error_code: error_code,
            sql_state: reader.read_bytes(5),
            error_message: reader.read_to_end()
        }
    }
}

impl fmt::Default for ErrPacket {
    fn fmt(obj: &ErrPacket, f: &mut fmt::Formatter) {
        write!(f.buf,
               "ERROR {:u} ({:s}): {:s}",
               obj.error_code,
               str::from_utf8(obj.sql_state),
               str::from_utf8(obj.error_message))
    }
}

/***
 *    8888888888  .d88888b.  8888888888 8888888b.                    888               888    
 *    888        d88P" "Y88b 888        888   Y88b                   888               888    
 *    888        888     888 888        888    888                   888               888    
 *    8888888    888     888 8888888    888   d88P  8888b.   .d8888b 888  888  .d88b.  888888 
 *    888        888     888 888        8888888P"      "88b d88P"    888 .88P d8P  Y8b 888    
 *    888        888     888 888        888        .d888888 888      888888K  88888888 888    
 *    888        Y88b. .d88P 888        888        888  888 Y88b.    888 "88b Y8b.     Y88b.  
 *    8888888888  "Y88888P"  888        888        "Y888888  "Y8888P 888  888  "Y8888   "Y888 
 *                                                                                            
 *                                                                                            
 *                                                                                            
 */

struct EOFPacket {
    warnings: u16,
    status_flags: u16
}

impl EOFPacket {
    #[inline]
    fn from_payload(pld: &[u8]) -> EOFPacket {
        let mut reader = BufReader::new(pld);
        let _ = reader.read_byte();
        EOFPacket{
            warnings: reader.read_le_u16(),
            status_flags: reader.read_le_u16()
        }
    }
}

/***
 *    888    888                        888          888               888               8888888b.                    888               888    
 *    888    888                        888          888               888               888   Y88b                   888               888    
 *    888    888                        888          888               888               888    888                   888               888    
 *    8888888888  8888b.  88888b.   .d88888 .d8888b  88888b.   8888b.  888  888  .d88b.  888   d88P  8888b.   .d8888b 888  888  .d88b.  888888 
 *    888    888     "88b 888 "88b d88" 888 88K      888 "88b     "88b 888 .88P d8P  Y8b 8888888P"      "88b d88P"    888 .88P d8P  Y8b 888    
 *    888    888 .d888888 888  888 888  888 "Y8888b. 888  888 .d888888 888888K  88888888 888        .d888888 888      888888K  88888888 888    
 *    888    888 888  888 888  888 Y88b 888      X88 888  888 888  888 888 "88b Y8b.     888        888  888 Y88b.    888 "88b Y8b.     Y88b.  
 *    888    888 "Y888888 888  888  "Y88888  88888P' 888  888 "Y888888 888  888  "Y8888  888        "Y888888  "Y8888P 888  888  "Y8888   "Y888 
 *                                                                                                                                             
 *                                                                                                                                             
 *                                                                                                                                             
 */

struct HandshakePacket {
    auth_plugin_data: ~[u8],
    auth_plugin_name: ~[u8],
    connection_id: u32,
    capability_flags: u32,
    status_flags: u16,
    protocol_version: u8,
    character_set: u8,
}

impl HandshakePacket {
    #[inline]
    fn from_payload(pld: &[u8]) -> HandshakePacket {
        let mut length_of_auth_plugin_data = 0i16;
        let mut auth_plugin_data: ~[u8] = vec::with_capacity(32);
        let mut auth_plugin_name: ~[u8] = vec::with_capacity(32);
        let mut character_set = 0u8;
        let mut status_flags = 0u16;
        let payload_len = pld.len();
        let mut reader = BufReader::new(pld);
        let protocol_version = reader.read_u8();
        // skip server version
        while reader.read_u8() != 0u8 {}
        let connection_id = reader.read_le_u32();
        reader.push_bytes(&mut auth_plugin_data, 8);
        // skip filler
        let _ = reader.read_byte();
        let mut capability_flags = reader.read_le_u16() as u32;
        if reader.tell() != payload_len as u64 {
            character_set = reader.read_u8();
            status_flags = reader.read_le_u16();
            capability_flags |= ((reader.read_le_u16() as u32) << 16);
            if (capability_flags & consts::CLIENT_PLUGIN_AUTH) > 0 {
                length_of_auth_plugin_data = reader.read_u8() as i16;
            } else {
                let _ = reader.read_byte();
            }
            // skip reserved bytes 0u8 x 10
            // TODO: seek not implemented
            // reader.seek(10, SeekCur);
            reader.read_bytes(10);
            if (capability_flags & consts::CLIENT_SECURE_CONNECTION) > 0 {
                let mut len = length_of_auth_plugin_data - 8i16;
                len = (if len > 13i16 { len } else { 13i16 });
                reader.push_bytes(&mut auth_plugin_data, len as uint);
                if auth_plugin_data[auth_plugin_data.len() - 1] == 0u8 {
                    auth_plugin_data.pop();
                }
            }
            if (capability_flags & consts::CLIENT_PLUGIN_AUTH) > 0 {
                auth_plugin_name = reader.read_to_end();
                if auth_plugin_name[auth_plugin_name.len() - 1] == 0u8 {
                    auth_plugin_name.pop();
                }
            }
        }
        HandshakePacket{protocol_version: protocol_version, connection_id: connection_id,
                         auth_plugin_data: auth_plugin_data,
                         capability_flags: capability_flags, character_set: character_set,
                         status_flags: status_flags, auth_plugin_name: auth_plugin_name}
    }
}

/***
 *     .d8888b.  888                  888    
 *    d88P  Y88b 888                  888    
 *    Y88b.      888                  888    
 *     "Y888b.   888888 88888b.d88b.  888888 
 *        "Y88b. 888    888 "888 "88b 888    
 *          "888 888    888  888  888 888    
 *    Y88b  d88P Y88b.  888  888  888 Y88b.  
 *     "Y8888P"   "Y888 888  888  888  "Y888 
 *                                           
 *                                           
 *                                           
 */

struct Stmt {
    params: Option<~[Column]>,
    columns: Option<~[Column]>,
    statement_id: u32,
    num_columns: u16,
    num_params: u16,
    warning_count: u16,
}

impl Stmt {
    #[inline]
    fn from_payload(pld: &[u8]) -> Stmt {
        let mut reader = BufReader::new(pld);
        let _ = reader.read_byte();
        let statement_id = reader.read_le_u32();
        let num_columns = reader.read_le_u16();
        let num_params = reader.read_le_u16();
        let warning_count = reader.read_le_u16();
        Stmt{statement_id: statement_id,
              num_columns: num_columns,
              num_params: num_params,
              warning_count: warning_count,
              params: None,
              columns: None}
    }
}

/***
 *     .d8888b.           888                                 
 *    d88P  Y88b          888                                 
 *    888    888          888                                 
 *    888         .d88b.  888 888  888 88888b.d88b.  88888b.  
 *    888        d88""88b 888 888  888 888 "888 "88b 888 "88b 
 *    888    888 888  888 888 888  888 888  888  888 888  888 
 *    Y88b  d88P Y88..88P 888 Y88b 888 888  888  888 888  888 
 *     "Y8888P"   "Y88P"  888  "Y88888 888  888  888 888  888 
 *                                                            
 *                                                            
 *                                                            
 */

#[deriving(Clone)]
struct Column {
    catalog: ~[u8],
    schema: ~[u8],
    table: ~[u8],
    org_table: ~[u8],
    name: ~[u8],
    org_name: ~[u8],
    default_values: ~[u8],
    column_length: u32,
    character_set: u16,
    flags: u16,
    column_type: u8,
    decimals: u8
}

impl Column {
    #[inline]
    fn from_payload(command: u8, pld: &[u8]) -> Column {
        let mut reader = BufReader::new(pld);
        let catalog = reader.read_lenenc_bytes();
        let schema = reader.read_lenenc_bytes();
        let table = reader.read_lenenc_bytes();
        let org_table = reader.read_lenenc_bytes();
        let name = reader.read_lenenc_bytes();
        let org_name = reader.read_lenenc_bytes();
        // skip next length
        let _ = reader.read_lenenc_int();
        let character_set = reader.read_le_u16();
        let column_length = reader.read_le_u32();
        let column_type = reader.read_u8();
        let flags = reader.read_le_u16();
        let decimals = reader.read_u8();
        // skip filler
        let _ = reader.read_bytes(2);
        let mut default_values = ~[];
        if command == consts::COM_FIELD_LIST {
            let len = reader.read_lenenc_int();
            default_values = reader.read_bytes(len as uint);
        }
        Column{catalog: catalog,
                schema: schema,
                table: table,
                org_table: org_table,
                name: name,
                org_name: org_name,
                character_set: character_set,
                column_length: column_length,
                column_type: column_type,
                flags: flags,
                decimals: decimals,
                default_values: default_values}
    }
}

/***
 *    888     888          888                   
 *    888     888          888                   
 *    888     888          888                   
 *    Y88b   d88P  8888b.  888 888  888  .d88b.  
 *     Y88b d88P      "88b 888 888  888 d8P  Y8b 
 *      Y88o88P   .d888888 888 888  888 88888888 
 *       Y888P    888  888 888 Y88b 888 Y8b.     
 *        Y8P     "Y888888 888  "Y88888  "Y8888  
 *                                               
 *                                               
 *                                               
 */

#[deriving(Clone, DeepClone, Eq, Ord, ToStr)]
pub enum Value {
    NULL,
    Bytes(~[u8]),
    Int(i64),
    UInt(u64),
    Float(f64),
    // year, month, day, hour, minutes, seconds, micro seconds
    Date(u16, u8, u8, u8, u8, u8, u32),
    // is negative, days, hours, minutes, seconds, micro seconds
    Time(bool, u32, u8, u8, u8, u32)
}

impl Value {
    /// Get correct string representation of a mysql value
    pub fn into_str(&self) -> ~str {
        match *self {
            NULL => ~"NULL",
            Bytes(ref x) => {
                if ! str::is_utf8(*x) {
                    let mut s = ~"0x";
                    for c in x.iter() {
                        s.push_str(format!("{:02X}", *c));
                    }
                    s
                } else {
                    let mut s = str::from_utf8_owned(x.clone());
                    s = str::replace(s, "'", "\'");
                    format!("'{:s}'", s)
                }
            },
            Int(x) => format!("{:d}", x),
            UInt(x) => format!("{:u}", x),
            Float(x) => format!("{:f}", x),
            Date(0, 0, 0, 0, 0, 0, 0) => ~"''",
            Date(y, m, d, 0, 0, 0, 0) => format!("'{:04u}-{:02u}-{:02u}'", y, m, d),
            Date(y, m, d, h, i, s, 0) => format!("'{:04u}-{:02u}-{:02u} {:02u}:{:02u}:{:02u}'", y, m, d, h, i, s),
            Date(y, m, d, h, i, s, u) => format!("'{:04u}-{:02u}-{:02u} {:02u}:{:02u}:{:02u}.{:06u}'", y, m, d, h, i, s, u),
            Time(_, 0, 0, 0, 0, 0) => ~"''",
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
    pub fn unwrap_bytes(self) -> ~[u8] {
        match self {
            Bytes(x) => x,
            _ => fail!("Called `Value::unwrap_bytes()` on non `Bytes` value")
        }
    }
    pub fn unwrap_bytes_or(self, y: ~[u8]) -> ~[u8] {
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
    // -> (~value, consumed_buf_len)
    fn from_bin(column: &Column, buf: &[u8]) -> (Value, uint) {
        let mut reader = BufReader::new(buf);
        match column.column_type {
            consts::MYSQL_TYPE_TINY => {
                if (column.flags & consts::UNSIGNED_FLAG) == 0 {
                    (Int(reader.read_i8() as i64), 1)
                } else {
                    (Int(reader.read_u8() as i64), 1)
                }
            },
            consts::MYSQL_TYPE_SHORT |
            consts::MYSQL_TYPE_YEAR => {
                if (column.flags & consts::UNSIGNED_FLAG) == 0 {
                    (Int(reader.read_le_i16() as i64), 2)
                } else {
                    (Int(reader.read_le_u16() as i64), 2)
                }
            },
            consts::MYSQL_TYPE_LONG |
            consts::MYSQL_TYPE_INT24 => {
                if (column.flags & consts::UNSIGNED_FLAG) == 0 {
                    (Int(reader.read_le_i32() as i64), 4)
                } else {
                    (Int(reader.read_le_u32() as i64), 4)
                }
            },
            consts::MYSQL_TYPE_LONGLONG => {
                if (column.flags & consts::UNSIGNED_FLAG) == 0 {
                    (Int(reader.read_le_i64() as i64), 8)
                } else {
                    (UInt(reader.read_le_u64()), 8)
                }
            },
            consts::MYSQL_TYPE_FLOAT => {
                (Float(reader.read_le_f32() as f64), 4)
            },
            consts::MYSQL_TYPE_DOUBLE => {
                (Float(reader.read_le_f64()), 8)
            },
            consts::MYSQL_TYPE_DECIMAL |
            consts::MYSQL_TYPE_VARCHAR |
            consts::MYSQL_TYPE_BIT |
            consts::MYSQL_TYPE_NEWDECIMAL |
            consts::MYSQL_TYPE_ENUM |
            consts::MYSQL_TYPE_SET |
            consts::MYSQL_TYPE_TINY_BLOB |
            consts::MYSQL_TYPE_MEDIUM_BLOB |
            consts::MYSQL_TYPE_LONG_BLOB |
            consts::MYSQL_TYPE_BLOB |
            consts::MYSQL_TYPE_VAR_STRING |
            consts::MYSQL_TYPE_STRING |
            consts::MYSQL_TYPE_GEOMETRY => {
                let bytes = reader.read_lenenc_bytes();
                let bytes_len = bytes.len();
                let lenenc_int_len = if bytes_len < 255 {
                    1
                } else if bytes_len < 65536 {
                    3
                } else if bytes_len < 16777216 {
                    4
                } else {
                    9
                };
                (Bytes(bytes), lenenc_int_len + bytes_len)
            },
            consts::MYSQL_TYPE_TIMESTAMP |
            consts::MYSQL_TYPE_DATE |
            consts::MYSQL_TYPE_DATETIME => {
                let len = reader.read_u8();
                let mut year = 0u16;
                let mut month = 0u8;
                let mut day = 0u8;
                let mut hour = 0u8;
                let mut minute = 0u8;
                let mut second = 0u8;
                let mut micro_second = 0u32;
                if len >= 4u8 {
                    year = reader.read_le_u16();
                    month = reader.read_u8();
                    day = reader.read_u8();
                }
                if len >= 7u8 {
                    hour = reader.read_u8();
                    minute = reader.read_u8();
                    second = reader.read_u8();
                }
                if len == 11u8 {
                    micro_second = reader.read_le_u32();
                }
                (Date(year, month, day, hour, minute, second, micro_second), (len + 1) as uint)
            },
            consts::MYSQL_TYPE_TIME => {
                let len = reader.read_u8();
                let mut is_negative = false;
                let mut days = 0u32;
                let mut hours = 0u8;
                let mut minutes = 0u8;
                let mut seconds = 0u8;
                let mut micro_seconds = 0u32;
                if len >= 8u8 {
                    is_negative = reader.read_u8() == 1u8;
                    days = reader.read_le_u32();
                    hours = reader.read_u8();
                    minutes = reader.read_u8();
                    seconds = reader.read_u8();
                }
                if len == 12u8 {
                    micro_seconds = reader.read_le_u32();
                }
                (Time(is_negative, days, hours, minutes, seconds, micro_seconds), (len + 1) as uint)
            }
            _ => (NULL, 0)
        }
    }
    fn to_bin(&self) -> ~[u8] {
        let mut writer = MemWriter::with_capacity(256);
        match *self {
            NULL => (),
            Bytes(ref x) => writer.write_lenenc_bytes(*x),
            Int(x) => writer.write_le_i64(x),
            UInt(x) => writer.write_le_u64(x),
            Float(x) => writer.write_le_f64(x),
            Date(0u16, 0u8, 0u8, 0u8, 0u8, 0u8, 0u32) => writer.write_u8(0u8),
            Date(y, m, d, 0u8, 0u8, 0u8, 0u32) => {
                writer.write_u8(4u8);
                writer.write_le_u16(y);
                writer.write_u8(m);
                writer.write_u8(d);
            },
            Date(y, m, d, h, i, s, 0u32) => {
                writer.write_u8(7u8);
                writer.write_le_u16(y);
                writer.write_u8(m);
                writer.write_u8(d);
                writer.write_u8(h);
                writer.write_u8(i);
                writer.write_u8(s);
            },
            Date(y, m, d, h, i, s, u) => {
                writer.write_u8(11u8);
                writer.write_le_u16(y);
                writer.write_u8(m);
                writer.write_u8(d);
                writer.write_u8(h);
                writer.write_u8(i);
                writer.write_u8(s);
                writer.write_le_u32(u);
            },
            Time(_, 0u32, 0u8, 0u8, 0u8, 0u32) => writer.write_u8(0u8),
            Time(neg, d, h, m, s, 0u32) => {
                writer.write_u8(8u8);
                writer.write_u8(if neg {1u8} else {0u8});
                writer.write_le_u32(d);
                writer.write_u8(h);
                writer.write_u8(m);
                writer.write_u8(s);
            },
            Time(neg, d, h, m, s, u) => {
                writer.write_u8(12u8);
                writer.write_u8(if neg {1u8} else {0u8});
                writer.write_le_u32(d);
                writer.write_u8(h);
                writer.write_u8(m);
                writer.write_u8(s);
                writer.write_le_u32(u);
            }
        }
        writer.inner()
    }
    #[inline]
    fn from_payload(pld: &[u8], columns_count: uint) -> ~[Value] {
        let mut output: ~[Value] = vec::with_capacity(columns_count);
        let mut reader = BufReader::new(pld);
        loop {
            if reader.eof() {
                break;
            } else if { let pos = reader.tell(); pld[pos] == 0xfb_u8 } {
                reader.read_byte();
                output.push(NULL);
            } else {
                output.push(Bytes(reader.read_lenenc_bytes()));
            }
        }
        output
    }
    #[inline]
    fn from_bin_payload(pld: &[u8], columns: &[Column]) -> ~[Value] {
        let bit_offset = 2; // http://dev.mysql.com/doc/internals/en/null-bitmap.html
        let bitmap_len = (columns.len() + 7 + bit_offset) / 8;
        let mut bitmap: ~[u8] = vec::with_capacity(bitmap_len);
        let mut values: ~[Value] = vec::with_capacity(columns.len());
        let mut reader = BufReader::new(pld);
        let _ = reader.read_byte();
        reader.push_bytes(&mut bitmap, bitmap_len);
        let mut i = -1;
        let mut offset = 1 + bitmap_len;
        while {i += 1; i < columns.len()} {
            if bitmap[(i + bit_offset) / 8] & (1 << ((i + bit_offset) % 8)) == 0 {
                let (val, len) = Value::from_bin(&columns[i], pld.slice_from(offset));
                values.push(val);
                offset += len;
            } else {
                values.push(NULL);
            }
        }
        values
    }
    // (NULL-bitmap, values, ids of fields to send throwgh send_long_data)
    fn to_bin_payload(params: &[Column], values: &[Value], max_allowed_packet: uint) -> (~[u8], ~[u8], Option<~[u16]>) {
        let bitmap_len = (params.len() + 7) / 8;
        let mut large_ids: ~[u16] = ~[];
        let mut writer = MemWriter::new();
        let mut bitmap = vec::from_elem(bitmap_len, 0u8);
        let mut i = -1;
        while {i += 1; i < params.len()} {
            let val = values[i].to_bin();
            if val.len() == 0 {
                bitmap[i / 8] |= 1 << (i % 8);
            } else {
                if val.len() < max_allowed_packet - writer.tell() as uint - bitmap_len - values.len()*2*32 - 11 {
                    writer.write(val);
                } else {
                    large_ids.push(i as u16);
                }
            }
        }
        if large_ids.len() == 0 {
            (bitmap, writer.inner(), None)
        } else {
            (bitmap, writer.inner(), Some(large_ids))
        }
    }
}

/***
 *    888b     d888           .d88888b.           888             
 *    8888b   d8888          d88P" "Y88b          888             
 *    88888b.d88888          888     888          888             
 *    888Y88888P888 888  888 888     888 88888b.  888888 .d8888b  
 *    888 Y888P 888 888  888 888     888 888 "88b 888    88K      
 *    888  Y8P  888 888  888 888     888 888  888 888    "Y8888b. 
 *    888   "   888 Y88b 888 Y88b. .d88P 888 d88P Y88b.       X88 
 *    888       888  "Y88888  "Y88888P"  88888P"   "Y888  88888P' 
 *                       888             888                      
 *                  Y8b d88P             888                      
 *                   "Y88P"              888                      
 */

pub struct MyOpts {
    tcp_addr: Option<SocketAddr>,
    unix_addr: Option<Path>,
    user: Option<~str>,
    pass: Option<~str>,
    db_name: Option<~str>
}

impl MyOpts {
    fn get_user(&self) -> ~str {
        match self.user {
            Some(ref x) => x.clone(),
            None => ~""
        }
    }
    fn get_pass(&self) -> ~str {
        match self.pass {
            Some(ref x) => x.clone(),
            None => ~""
        }
    }
    fn get_db_name(&self) -> ~str {
        match self.db_name {
            Some(ref x) => x.clone(),
            None => ~""
        }
    }
}

impl Default for MyOpts {
    fn default() -> MyOpts {
        MyOpts{tcp_addr: Some(SocketAddr{ip: Ipv4Addr(127, 0, 0, 1), port: 3306}),
               unix_addr: None,
               user: None,
               pass: None,
               db_name: None}
    }
}

/***
 *    888b     d888           .d8888b.                             
 *    8888b   d8888          d88P  Y88b                            
 *    88888b.d88888          888    888                            
 *    888Y88888P888 888  888 888         .d88b.  88888b.  88888b.  
 *    888 Y888P 888 888  888 888        d88""88b 888 "88b 888 "88b 
 *    888  Y8P  888 888  888 888    888 888  888 888  888 888  888 
 *    888   "   888 Y88b 888 Y88b  d88P Y88..88P 888  888 888  888 
 *    888       888  "Y88888  "Y8888P"   "Y88P"  888  888 888  888 
 *                       888                                       
 *                  Y8b d88P                                       
 *                   "Y88P"                                        
 */

pub struct MyConn {
    opts: MyOpts,
    stream: ~Stream,
    affected_rows: u64,
    last_insert_id: u64,
    max_allowed_packet: uint,
    capability_flags: u32,
    connection_id: u32,
    status_flags: u16,
    seq_id: u8,
    character_set: u8,
    last_command: u8
}

impl MyConn {
    pub fn new(opts: MyOpts) -> Result<MyConn, ~str> {
        if opts.unix_addr.is_some() {
            let unix_stream = UnixStream::connect(opts.unix_addr.get_ref());
            if unix_stream.is_some() {
                return Ok(MyConn{
                    stream: ~(unix_stream.unwrap()) as ~Stream,
                    seq_id: 0u8,
                    capability_flags: 0,
                    status_flags: 0u16,
                    connection_id: 0u32,
                    character_set: 0u8,
                    affected_rows: 0u64,
                    last_insert_id: 0u64,
                    last_command: 0u8,
                    max_allowed_packet: consts::MAX_PAYLOAD_LEN,
                    opts: opts
                });
            } else {
                return Err(format!("Could not connect to address: {:?}", opts.unix_addr));
            }
        }
        if opts.tcp_addr.is_some() {
            let tcp_stream = TcpStream::connect(opts.tcp_addr.unwrap());
            if tcp_stream.is_some() {
                return Ok(MyConn{
                    stream: ~(tcp_stream.unwrap()) as ~Stream,
                    seq_id: 0u8,
                    capability_flags: 0,
                    status_flags: 0u16,
                    connection_id: 0u32,
                    character_set: 0u8,
                    affected_rows: 0u64,
                    last_insert_id: 0u64,
                    last_command: 0u8,
                    max_allowed_packet: consts::MAX_PAYLOAD_LEN,
                    opts: opts
                });
            } else {
                return Err(format!("Could not connect to address: {:?}", opts.tcp_addr));
            }
        } else {
            return Err(~"Could not connect. Address not specified");
        }
    }
}

impl Reader for MyConn {
    fn read(&mut self, buf: &mut [u8]) -> Option<uint> {
        self.stream.read(buf)
    }
    fn eof(&mut self) -> bool {
        self.stream.eof()
    }
}
impl Reader for ~MyConn {
    fn read(&mut self, buf: &mut [u8]) -> Option<uint> {
        self.read(buf)
    }
    fn eof(&mut self) -> bool {
        self.eof()
    }
}
impl<'a> Reader for &'a MyConn {
    fn read(&mut self, buf: &mut [u8]) -> Option<uint> {
        self.read(buf)
    }
    fn eof(&mut self) -> bool {
        self.eof()
    }
}

impl Writer for MyConn {
    fn write(&mut self, buf: &[u8]) {
        self.stream.write(buf)
    }
}
impl Writer for ~MyConn {
    fn write(&mut self, buf: &[u8]) {
        self.write(buf)
    }
}
impl<'a> Writer for &'a MyConn {
    fn write(&mut self, buf: &[u8]) {
        self.write(buf)
    }
}

/***
 *    888b     d888          8888888b.                         888                  
 *    8888b   d8888          888   Y88b                        888                  
 *    88888b.d88888          888    888                        888                  
 *    888Y88888P888 888  888 888   d88P  .d88b.   8888b.   .d88888  .d88b.  888d888 
 *    888 Y888P 888 888  888 8888888P"  d8P  Y8b     "88b d88" 888 d8P  Y8b 888P"   
 *    888  Y8P  888 888  888 888 T88b   88888888 .d888888 888  888 88888888 888     
 *    888   "   888 Y88b 888 888  T88b  Y8b.     888  888 Y88b 888 Y8b.     888     
 *    888       888  "Y88888 888   T88b  "Y8888  "Y888888  "Y88888  "Y8888  888     
 *                       888                                                        
 *                  Y8b d88P                                                        
 *                   "Y88P"                                                         
 */

pub trait MyReader: Reader {
    #[inline]
    fn read_lenenc_int(&mut self) -> u64 {
        let head_byte = self.read_u8();
        let mut length;
        match head_byte {
            0xfc => {
                length = 2;
            },
            0xfd => {
                length = 3;
            },
            0xfe => {
                length = 8;
            },
            x => {
                return x as u64;
            }
        }
        return self.read_le_uint_n(length);
    }
    #[inline]
    fn read_lenenc_bytes(&mut self) -> ~[u8] {
        let len = self.read_lenenc_int();
        if len > 0 {
            self.read_bytes(len as uint)
        } else {
            ~[]
        }
    }
    #[inline]
    fn read_to_nul(&mut self) -> ~[u8] {
        let mut buf = ~[];
        let mut x = 0u8;
        while { x = self.read_u8(); x != 0u8 } {
            buf.push(x);
        }
        buf
    }
}

impl<T:Reader> MyReader for T {}

/***
 *    888b     d888          888       888         d8b 888                     
 *    8888b   d8888          888   o   888         Y8P 888                     
 *    88888b.d88888          888  d8b  888             888                     
 *    888Y88888P888 888  888 888 d888b 888 888d888 888 888888  .d88b.  888d888 
 *    888 Y888P 888 888  888 888d88888b888 888P"   888 888    d8P  Y8b 888P"   
 *    888  Y8P  888 888  888 88888P Y88888 888     888 888    88888888 888     
 *    888   "   888 Y88b 888 8888P   Y8888 888     888 Y88b.  Y8b.     888     
 *    888       888  "Y88888 888P     Y888 888     888  "Y888  "Y8888  888     
 *                       888                                                   
 *                  Y8b d88P                                                   
 *                   "Y88P"                                                    
 */

pub trait MyWriter: Writer {
    #[inline]
    fn write_le_uint_n(&mut self, x: u64, len: uint) {
        let mut buf = vec::from_elem(len, 0u8);
        let mut offset = -1;
        while { offset += 1; offset < len } {
            buf[offset] = (((0xff << (offset * 8)) & x) >> (offset * 8)) as u8;
        }
        self.write(buf)
    }
    #[inline]
    fn write_lenenc_int(&mut self, x: u64) {
        if x < 251u64 {
            self.write_le_uint_n(x, 1)
        } else if x < 65536u64 {
            self.write_u8(0xfc_u8);
            self.write_le_uint_n(x, 2)
        } else if x < 16_777_216u64 {
            self.write_u8(0xfd_u8);
            self.write_le_uint_n(x, 3)
        } else {
            self.write_u8(0xfe_u8);
            self.write_le_uint_n(x, 8)
        }
    }
    #[inline]
    fn write_lenenc_bytes(&mut self, bytes: &[u8]) {
        self.write_lenenc_int(bytes.len() as u64);
        self.write(bytes)
    }
}

impl<T: Writer> MyWriter for T {}

/***
 *    888b     d888           .d8888b.  888                                            
 *    8888b   d8888          d88P  Y88b 888                                            
 *    88888b.d88888          Y88b.      888                                            
 *    888Y88888P888 888  888  "Y888b.   888888 888d888  .d88b.   8888b.  88888b.d88b.  
 *    888 Y888P 888 888  888     "Y88b. 888    888P"   d8P  Y8b     "88b 888 "888 "88b 
 *    888  Y8P  888 888  888       "888 888    888     88888888 .d888888 888  888  888 
 *    888   "   888 Y88b 888 Y88b  d88P Y88b.  888     Y8b.     888  888 888  888  888 
 *    888       888  "Y88888  "Y8888P"   "Y888 888      "Y8888  "Y888888 888  888  888 
 *                       888                                                           
 *                  Y8b d88P                                                           
 *                   "Y88P"                                                            
 */

pub trait MyStream: MyReader + MyWriter {
    fn read_packet(&mut self) -> Result<~[u8], ~str>;
    fn write_packet(&mut self, data: &[u8]) -> Result<(), ~str>;
    fn handle_ok(&mut self, ok: &OkPacket);
    fn handle_eof(&mut self, eof: &EOFPacket);
    fn handle_handshake(&mut self, hp: &HandshakePacket);
    fn do_handshake(&mut self) -> Result<(), ~str>;
    fn do_handshake_response(&mut self, hp: &HandshakePacket) -> Result<(), ~str>;
    fn write_command(&mut self, cmd: u8) -> Result<(), ~str>;
    fn write_command_data(&mut self, cmd: u8, buf: &[u8]) -> Result<(), ~str>;
    fn send_local_infile(&mut self, file_name: &[u8]) -> Result<(), ~str>;
    fn query<'a>(&'a mut self, query: &str) -> Result<Option<MyResult<'a>>, ~str>;
    fn prepare(&mut self, query: &str) -> Result<Stmt, ~str>;
    fn send_long_data(&mut self, stmt: &Stmt, params: &[Value], ids: ~[u16]);
    fn execute<'a>(&'a mut self, stmt: &Stmt, params: &[Value]) -> Result<Option<MyResult<'a>>, ~str>;
    fn connect(&mut self) -> Result<(), ~str>;
}

impl MyStream for MyConn {
    fn read_packet(&mut self) -> Result<~[u8], ~str> {
        let mut output = vec::with_capacity(256);
        loop {
            let payload_len = self.read_le_uint_n(3);
            let seq_id = self.read_u8();
            if seq_id != self.seq_id {
                return Err(~"Packet out of sync");
            }
            self.seq_id += 1;
            if payload_len as uint >= consts::MAX_PAYLOAD_LEN {
                self.push_bytes(&mut output, consts::MAX_PAYLOAD_LEN);
            } else if payload_len == 0 {
                break;
            } else {
                self.push_bytes(&mut output, payload_len as uint);
                break;
            }
        }
        Ok(output)
    }
    fn write_packet(&mut self, data: &[u8]) -> Result<(), ~str> {
        if data.len() > self.max_allowed_packet && self.max_allowed_packet < consts::MAX_PAYLOAD_LEN {
            return Err(~"Packet too large");
        }
        if data.len() == 0 {
            self.write([0u8, 0u8, 0u8, self.seq_id]);
            self.seq_id += 1;
            return Ok(());
        }
        let mut last_was_max = false;
        for chunk in data.chunks(consts::MAX_PAYLOAD_LEN) {
            let chunk_len = chunk.len();
            let full_chunk_len = 4 + chunk_len;
            let mut full_chunk: ~[u8] = vec::from_elem(full_chunk_len, 0u8);
            if chunk_len == consts::MAX_PAYLOAD_LEN {
                last_was_max = true;
                full_chunk[0] = 255u8;
                full_chunk[1] = 255u8;
                full_chunk[2] = 255u8;
            } else {
                last_was_max = false;
                full_chunk[0] = (chunk_len & 255) as u8;
                full_chunk[1] = ((chunk_len & (255 << 8)) >> 8) as u8;
                full_chunk[2] = ((chunk_len & (255 << 16)) >> 16) as u8;
            }
            full_chunk[3] = self.seq_id;
            self.seq_id += 1;
            unsafe {
                let payload_slice = full_chunk.mut_slice_from(4);
                payload_slice.copy_memory(chunk);
            }
            self.write(full_chunk);
        }
        if last_was_max {
            self.write([0u8, 0u8, 0u8, self.seq_id]);
            self.seq_id += 1;
        }
        Ok(())
    }
    fn handle_handshake(&mut self, hp: &HandshakePacket) {
        self.capability_flags = hp.capability_flags;
        self.status_flags = hp.status_flags;
        self.connection_id = hp.connection_id;
        self.character_set = hp.character_set;
    }
    fn handle_ok(&mut self, op: &OkPacket) {
        self.affected_rows = op.affected_rows;
        self.last_insert_id = op.last_insert_id;
        self.status_flags = op.status_flags;
    }
    fn handle_eof(&mut self, eof: &EOFPacket) {
        self.status_flags = eof.status_flags;
    }
    fn do_handshake(&mut self) -> Result<(), ~str> {
        let pld = match self.read_packet() {
            Ok(pld) => pld,
            Err(error) => return Err(error)
        };
        let handshake = HandshakePacket::from_payload(pld);
        if handshake.protocol_version != 10u8 {
            return Err(format!("Unsupported protocol version {:u}", handshake.protocol_version));
        }
        if (handshake.capability_flags & consts::CLIENT_PROTOCOL_41) == 0 {
            return Err(~"Server must set CLIENT_PROTOCOL_41 flag");
        }
        self.handle_handshake(&handshake);
        self.do_handshake_response(&handshake);
        match self.read_packet() {
            Err(error) => return Err(error),
            Ok(pld) => {
                match pld[0] {
                    0u8 => {
                        let ok = OkPacket::from_payload(pld);
                        self.handle_ok(&ok);
                        return Ok(());
                    }
                    0xffu8 => {
                        let err = ErrPacket::from_payload(pld);
                        return Err(format!("{}", err));
                    },
                    _ => return Err(~"Unexpected packet")
                }
            }
        };
    }
    fn do_handshake_response(&mut self, hp: &HandshakePacket) -> Result<(), ~str> {
        let mut client_flags = consts::CLIENT_PROTOCOL_41 |
                           consts::CLIENT_SECURE_CONNECTION |
                           consts::CLIENT_LONG_PASSWORD |
                           consts::CLIENT_TRANSACTIONS |
                           consts::CLIENT_LOCAL_FILES |
                           (self.capability_flags & consts::CLIENT_LONG_FLAG);
        let scramble_buf = scramble(hp.auth_plugin_data, self.opts.get_pass().into_bytes());
        let mut payload_len = 4 + 4 + 1 + 23 + self.opts.get_user().len() + 1 + 1 + scramble_buf.len();
        if self.opts.get_db_name().len() > 0 {
            client_flags |= consts::CLIENT_CONNECT_WITH_DB;
            payload_len += self.opts.get_db_name().len() + 1;
        }

        let mut writer = MemWriter::with_capacity(payload_len);
        writer.write_le_u32(client_flags);
        writer.write_le_u32(0u32);
        writer.write_u8(consts::UTF8_GENERAL_CI);
        writer.write(~[0u8, ..23]);
        writer.write_str(self.opts.get_user());
        writer.write_u8(0u8);
        writer.write_u8(scramble_buf.len() as u8);
        writer.write(scramble_buf);
        if self.opts.get_db_name().len() > 0 {
            writer.write_str(self.opts.get_db_name());
            writer.write_u8(0u8);
        }

        self.write_packet(writer.inner())
    }
    fn write_command(&mut self, cmd: u8) -> Result<(), ~str> {
        self.seq_id = 0u8;
        self.last_command = cmd;
        self.write_packet([cmd])
    }
    fn write_command_data(&mut self, cmd: u8, buf: &[u8]) -> Result<(), ~str> {
        self.seq_id = 0u8;
        self.last_command = cmd;
        self.write_packet(vec::append(~[cmd], buf))
    }
    fn send_long_data(&mut self, stmt: &Stmt, params: &[Value], ids: ~[u16]) {
        for &id in ids.iter() {
            match params[id] {
                Bytes(ref x) => {
                    for chunk in x.chunks(self.max_allowed_packet - 7) {
                        let chunk_len = chunk.len() + 7;
                        let mut writer = MemWriter::with_capacity(chunk_len);
                        writer.write_le_u32(stmt.statement_id);
                        writer.write_le_u16(id);
                        writer.write(chunk);
                        self.write_command_data(consts::COM_STMT_SEND_LONG_DATA, writer.inner());
                    }
                },
                _ => (/* quite strange so do nothing */)
            }
        }
    }
    fn execute<'a>(&'a mut self, stmt: &Stmt, params: &[Value]) -> Result<Option<MyResult<'a>>, ~str> {
        if stmt.num_params != params.len() as u16 {
            return Err(format!("Statement takes {:u} parameters but {:u} was supplied", stmt.num_params, params.len()));
        }
        self.seq_id = 0u8;
        let mut writer = MemWriter::new();
        writer.write_le_u32(stmt.statement_id);
        writer.write_u8(0u8);
        writer.write_le_u32(1u32);
        if stmt.num_params > 0 {
            let (bitmap, values, large_ids) = Value::to_bin_payload(*stmt.params.get_ref(),
                                                                    params,
                                                                    self.max_allowed_packet);
            if large_ids.is_some() {
                self.send_long_data(stmt, params, large_ids.unwrap());
            }
            writer.write(bitmap);
            writer.write_u8(1u8);
            let mut i = -1;
            while { i += 1; i < params.len() } {
                match params[i] {
                    NULL => writer.write([stmt.params.get_ref()[i].column_type, 0u8]),
                    Bytes(..) => writer.write([consts::MYSQL_TYPE_VAR_STRING, 0u8]),
                    Int(..) => writer.write([consts::MYSQL_TYPE_LONGLONG, 0u8]),
                    UInt(..) => writer.write([consts::MYSQL_TYPE_LONGLONG, 128u8]),
                    Float(..) => writer.write([consts::MYSQL_TYPE_DOUBLE, 0u8]),
                    Date(..) => writer.write([consts::MYSQL_TYPE_DATE, 0u8]),
                    Time(..) => writer.write([consts::MYSQL_TYPE_TIME, 0u8])
                }
            }
            writer.write(values);
        }
        self.write_command_data(consts::COM_STMT_EXECUTE, writer.inner());
        let pld = match self.read_packet() {
            Ok(pld) => pld,
            Err(error) => return Err(error)
        };
        match pld[0] {
            0u8 => {
                let ok = OkPacket::from_payload(pld);
                self.handle_ok(&ok);
                return Ok(None);
            },
            0xffu8 => {
                let err = ErrPacket::from_payload(pld);
                return Err(format!("{}", err));
            },
            _ => {
                let mut reader = BufReader::new(pld);
                let column_count = reader.read_lenenc_int();
                let mut columns: ~[Column] = vec::with_capacity(column_count as uint);
                let mut i = -1;
                while { i += 1; i < column_count } {
                    let pld = match self.read_packet() {
                        Ok(pld) => pld,
                        Err(error) => return Err(error)
                    };
                    columns.push(Column::from_payload(self.last_command, pld));
                }
                // TODO http://dev.mysql.com/doc/internals/en/binary-protocol-resultset.html
                let _ = self.read_packet();
                return Ok(Some(MyResult{conn: self,
                                        columns: columns,
                                        eof: false,
                                        is_bin: true}))
            }
        }
    }
    fn send_local_infile(&mut self, file_name: &[u8]) -> Result<(), ~str> {
        let path = Path::new(file_name);
        let file = File::open(&path);
        if file.is_none() {
            return Err(format!("Could not open local file {}", path.display()));
        }
        let mut file = file.unwrap();
        let mut chunk = vec::from_elem(self.max_allowed_packet, 0u8);
        let mut count: Option<uint> = None;
        while { count = file.read(chunk); count.is_some() && *count.get_ref() > 0 } {
            self.write_packet(chunk.slice_to(count.unwrap()));
        }
        self.write_packet([]);
        let pld = match self.read_packet() {
            Ok(pld) => pld,
            Err(error) => return Err(error)
        };
        if pld[0] == 0u8 {
            self.handle_ok(&OkPacket::from_payload(pld));
        }
        Ok(())
    }
    fn query<'a>(&'a mut self, query: &str) -> Result<Option<MyResult<'a>>, ~str> {
        match self.write_command_data(consts::COM_QUERY, query.as_bytes()) {
            Err(err) => return Err(err),
            Ok(_) => {
                let pld = match self.read_packet() {
                    Ok(pld) => pld,
                    Err(error) => return Err(error)
                };
                match pld[0] {
                    0u8 => {
                        let ok = OkPacket::from_payload(pld);
                        self.handle_ok(&ok);
                        return Ok(None);
                    },
                    0xfb_u8 => {
                        let mut reader = BufReader::new(pld);
                        let _ = reader.read_u8();
                        let file_name = reader.read_to_end();
                        return match self.send_local_infile(file_name) {
                            Ok(..) => Ok(None),
                            Err(err) => Err(err)
                        };
                    },
                    0xff_u8 => {
                        let err = ErrPacket::from_payload(pld);
                        return Err(format!("{}", err));
                    },
                    _ => {
                        let mut reader = BufReader::new(pld);
                        let column_count = reader.read_lenenc_int();
                        let mut columns: ~[Column] = vec::with_capacity(column_count as uint);
                        let mut i = -1;
                        while { i += 1; i < column_count } {
                            let pld = match self.read_packet() {
                                Ok(pld) => pld,
                                Err(error) => return Err(error)
                            };
                            columns.push(Column::from_payload(self.last_command, pld));
                        }
                        // skip eof packet
                        let _ = self.read_packet();
                        return Ok(Some(MyResult{conn: self,
                                                 columns: columns,
                                                 eof: false,
                                                 is_bin: false}))
                    }
                }
            }
        }
    }
    fn prepare(&mut self, query: &str) -> Result<Stmt, ~str> {
        match self.write_command_data(consts::COM_STMT_PREPARE, query.as_bytes()) {
            Err(err) => return Err(err),
            Ok(_) => {
                let pld = match self.read_packet() {
                    Ok(pld) => pld,
                    Err(error) => return Err(error)
                };
                match pld[0] {
                    0xff => {
                        let err = ErrPacket::from_payload(pld);
                        return Err(format!("{}", err));
                    },
                    _ => {
                        let mut stmt = Stmt::from_payload(pld);
                        if stmt.num_params > 0 {
                            let mut params: ~[Column] = vec::with_capacity(stmt.num_params as uint);
                            let mut i = -1;
                            while { i += 1; i < stmt.num_params } {
                                let pld = match self.read_packet() {
                                    Ok(pld) => pld,
                                    Err(error) => return Err(error)
                                };
                                params.push(Column::from_payload(self.last_command, pld));
                            }
                            stmt.params = Some(params);
                            let _ = self.read_packet();
                        }
                        if stmt.num_columns > 0 {
                            let mut columns: ~[Column] = vec::with_capacity(stmt.num_columns as uint);
                            let mut i = -1;
                            while { i += 1; i < stmt.num_columns } {
                                let pld = match self.read_packet() {
                                    Ok(pld) => pld,
                                    Err(error) => return Err(error)
                                };
                                columns.push(Column::from_payload(self.last_command, pld));
                            }
                            stmt.columns = Some(columns);
                            let _ = self.read_packet();
                        }
                        return Ok(stmt);
                    }
                }
            }
        }
    }
    fn connect(&mut self) -> Result<(), ~str> {
        let mut max_allowed_packet = 0;
        match self.do_handshake() {
            Err(err) => {
                return Err(err)
            },
            Ok(_) => {
                let mut query_result = &mut self.query("SELECT @@max_allowed_packet");
                if query_result.is_ok() {;
                    let next_opt = query_result.next();
                    if next_opt.is_some() {
                        let next_result = next_opt.unwrap();
                        if next_result.is_ok() {
                            let next = next_result.unwrap();
                            match next[0] {
                                Bytes(ref val) => {
                                    max_allowed_packet = uint::parse_bytes(*val, 10).unwrap_or(0);
                                    if max_allowed_packet > consts::MAX_PAYLOAD_LEN {
                                        max_allowed_packet = consts::MAX_PAYLOAD_LEN
                                    }
                                },
                                _ => ()
                            }
                        }
                    }
                }
            }
        }
        if max_allowed_packet == 0 {
            Err(~"Can't get max_allowed_packet value")
        } else {
            self.max_allowed_packet = max_allowed_packet;
            Ok(())
        }
    }
}

/***
 *    888b     d888          8888888b.                             888 888    
 *    8888b   d8888          888   Y88b                            888 888    
 *    88888b.d88888          888    888                            888 888    
 *    888Y88888P888 888  888 888   d88P  .d88b.  .d8888b  888  888 888 888888 
 *    888 Y888P 888 888  888 8888888P"  d8P  Y8b 88K      888  888 888 888    
 *    888  Y8P  888 888  888 888 T88b   88888888 "Y8888b. 888  888 888 888    
 *    888   "   888 Y88b 888 888  T88b  Y8b.          X88 Y88b 888 888 Y88b.  
 *    888       888  "Y88888 888   T88b  "Y8888   88888P'  "Y88888 888  "Y888 
 *                       888                                                  
 *                  Y8b d88P                                                  
 *                   "Y88P"                                                   
 */

pub struct MyResult<'a> {
    conn: &'a mut MyConn,
    columns: ~[Column],
    eof: bool,
    is_bin: bool
}

impl<'a> MyResult<'a> {
    pub fn next(&mut self) -> Option<Result<~[Value], ~str>> {
        if self.eof {
            return None
        }
        let pld = match self.conn.read_packet() {
                Err(err) => {
                    self.eof = true;
                    return Some(Err(err));
                },
                Ok(pld) => pld
        };
        if self.is_bin {
            if pld[0] == 0xfe && pld.len() < 0xfe {
                self.eof = true;
                self.conn.handle_eof(&EOFPacket::from_payload(pld));
                return None;
            }
            Some(Ok(Value::from_bin_payload(pld, self.columns)))
        } else {
            if (pld[0] == 0xfe_u8 || pld[0] == 0xff_u8) && pld.len() < 0xfe {
                self.eof = true;
                if pld[0] == 0xfe_u8 {
                    self.conn.handle_eof(&EOFPacket::from_payload(pld));
                    return None;
                } else if pld[0] == 0xff_u8 {
                    return Some(Err(format!("{}", ErrPacket::from_payload(pld))));
                }
            }
            Some(Ok(Value::from_payload(pld, self.columns.len())))
        }
    }
}

#[unsafe_destructor]
impl<'a> Drop for MyResult<'a> {
    fn drop(&mut self) {
        for _ in *self {}
    }
}

impl<'a> Iterator<Result<~[Value], ~str>> for &'a mut Result<Option<MyResult<'a>>, ~str> {
    fn next(&mut self) -> Option<Result<~[Value], ~str>> {
        if self.is_ok() {
            let result_opt = self.as_mut().unwrap();
            if result_opt.is_some() {
                let result = result_opt.as_mut().unwrap();
                return result.next();
            }
        }
        return None;
    }
}

/***
 *    88888888888                   888             
 *        888                       888             
 *        888                       888             
 *        888      .d88b.  .d8888b  888888 .d8888b  
 *        888     d8P  Y8b 88K      888    88K      
 *        888     88888888 "Y8888b. 888    "Y8888b. 
 *        888     Y8b.          X88 Y88b.       X88 
 *        888      "Y8888   88888P'  "Y888  88888P' 
 *                                                  
 *                                                  
 *                                                  
 */

#[cfg(test)]
mod test {
    extern mod extra;
    use std::str;
    use std::vec;
    use std::os::{getcwd};
    use std::io::fs::{File, unlink};
    use std::path::posix::{Path};
    use conn::{OkPacket, ErrPacket, EOFPacket, HandshakePacket,
               MyConn, MyStream, MyOpts,
               Bytes, Int, UInt, Date, Time, Float, NULL};

    #[test]
    fn test_ok_packet() {
        let payload = ~[0u8, 1u8, 2u8, 3u8, 0u8, 4u8, 0u8, 32u8];
        let ok_packet = OkPacket::from_payload(payload);
        assert!(ok_packet.affected_rows == 1);
        assert!(ok_packet.last_insert_id == 2);
        assert!(ok_packet.status_flags == 3);
        assert!(ok_packet.warnings == 4);
        assert!(ok_packet.info == ~[32u8]);
    }

    #[test]
    fn test_err_packet() {
        let payload = ~[255u8, 1u8, 0u8, 35u8, 51u8, 68u8, 48u8, 48u8, 48u8, 32u8, 32u8];
        let err_packet = ErrPacket::from_payload(payload);
        assert!(err_packet.error_code == 1);
        assert!(err_packet.sql_state == ~[51u8, 68u8, 48u8, 48u8, 48u8]);
        assert!(err_packet.error_message == ~[32u8, 32u8]);
    }

    #[test]
    fn test_eof_packet() {
        let payload = ~[0xfe_u8, 1u8, 0u8, 2u8, 0u8];
        let eof_packet = EOFPacket::from_payload(payload);
        assert!(eof_packet.warnings == 1);
        assert!(eof_packet.status_flags == 2);
    }

    #[test]
    fn test_handshake_packet() {
        let mut payload = ~[0x0a_u8,
                        32u8, 32u8, 32u8, 32u8, 0u8,
                        1u8, 0u8, 0u8, 0u8,
                        1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8,
                        0u8,
                        3u8, 0x80_u8];
        let handshake_packet = HandshakePacket::from_payload(payload);
        assert!(handshake_packet.protocol_version == 0x0a);
        assert!(handshake_packet.connection_id == 1);
        assert!(handshake_packet.auth_plugin_data == ~[1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8]);
        assert!(handshake_packet.capability_flags == 0x00008003);
        payload.push(33u8);
        payload.push_all_move(~[4u8, 0u8]);
        payload.push_all_move(~[0x08_u8, 0u8]);
        payload.push_all_move(~[0x15_u8]);
        payload.push_all_move(::std::vec::from_elem(10, 0u8));
        payload.push_all_move(~[0x26_u8, 0x3a_u8, 0x34_u8, 0x34_u8, 0x46_u8, 0x44_u8,
                                0x63_u8, 0x44_u8, 0x69_u8, 0x63_u8, 0x39_u8, 0x30_u8, 0x00_u8]);
        payload.push_all_move(~[1u8, 2u8, 3u8, 4u8, 5u8, 0u8]);
        let handshake_packet = HandshakePacket::from_payload(payload);
        assert!(handshake_packet.protocol_version == 0x0a);
        assert!(handshake_packet.connection_id == 1);
        assert!(handshake_packet.auth_plugin_data == ~[1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8,
                                                       0x26_u8, 0x3a_u8, 0x34_u8, 0x34_u8, 0x46_u8, 0x44_u8,
                                                       0x63_u8, 0x44_u8, 0x69_u8, 0x63_u8, 0x39_u8, 0x30_u8]);
        assert!(handshake_packet.capability_flags == 0x00088003);
        assert!(handshake_packet.character_set == 33);
        assert!(handshake_packet.status_flags == 4);
        assert!(handshake_packet.auth_plugin_name == ~[1u8, 2u8, 3u8, 4u8, 5u8]);
    }

    #[test]
    fn test_value_into_str() {
        let v = NULL;
        assert!(v.into_str() == ~"NULL");
        let v = Bytes((~"hello").into_bytes());
        assert!(v.into_str() == ~"'hello'");
        let v = Bytes((~"h'e'l'l'o").into_bytes());
        assert!(v.into_str() == ~"'h\'e\'l\'l\'o'");
        let v = Bytes(~[0, 1, 2, 3, 4, 255]);
        assert!(v.into_str() == ~"0x0001020304FF");
        let v = Int(-65536);
        assert!(v.into_str() == ~"-65536");
        let v = UInt(4294967296);
        assert!(v.into_str() == ~"4294967296");
        let v = Float(686.868);
        assert!(v.into_str() == ~"686.868");
        let v = Date(0, 0, 0, 0, 0, 0, 0);
        assert!(v.into_str() == ~"''");
        let v = Date(2014, 2, 20, 0, 0, 0, 0);
        assert!(v.into_str() == ~"'2014-02-20'");
        let v = Date(2014, 2, 20, 22, 0, 0, 0);
        assert!(v.into_str() == ~"'2014-02-20 22:00:00'");
        let v = Date(2014, 2, 20, 22, 0, 0, 1);
        assert!(v.into_str() == ~"'2014-02-20 22:00:00.000001'");
        let v = Time(false, 0, 0, 0, 0, 0);
        assert!(v.into_str() == ~"''");
        let v = Time(true, 34, 3, 2, 1, 0);
        assert!(v.into_str() == ~"'-34 003:02:01'");
        let v = Time(false, 10, 100, 20, 30, 40);
        assert!(v.into_str() == ~"'10 100:20:30.000040'");
    }

    #[test]
    fn test_connect() {
        let mut conn = MyConn::new(MyOpts{user: Some(~"root"),
                                          pass: Some(~"password"),
                                          ..Default::default()}).unwrap();
        assert!(conn.connect().is_ok());
    }

    #[test]
    fn test_connect_with_db() {
        let mut conn = MyConn::new(MyOpts{user: Some(~"root"),
                                          pass: Some(~"password"),
                                          db_name: Some(~"mysql"),
                                          ..Default::default()}).unwrap();
        assert!(conn.connect().is_ok());
        for x in &mut conn.query("SELECT DATABASE()") {
            assert!(x.unwrap()[0].unwrap_bytes() == (~"mysql").into_bytes());
        }
    }

    #[test]
    fn test_query() {
        let mut conn = MyConn::new(MyOpts{user: Some(~"root"),
                                          pass: Some(~"password"),
                                          ..Default::default()}).unwrap();
        assert!(conn.connect().is_ok());
        assert!(conn.query("DROP DATABASE IF EXISTS test").is_ok());
        assert!(conn.query("CREATE DATABASE test").is_ok());
        assert!(conn.query("USE test").is_ok());
        assert!(conn.query("CREATE TABLE tbl(a TEXT, b INT, c INT UNSIGNED, d DATE, e FLOAT)").is_ok());
        assert!(conn.query("INSERT INTO tbl(a, b, c, d, e) VALUES ('hello', -123, 123, '2014-05-05', 123.123)").is_ok());
        assert!(conn.query("INSERT INTO tbl(a, b, c, d, e) VALUES ('world', -321, 321, '2014-06-06', 321.321)").is_ok());
        assert!(conn.query("SELECT * FROM unexisted").is_err());
        assert!(conn.query("SELECT * FROM tbl").is_ok());
        // Drop
        assert!(conn.query("SELECT * FROM tbl").is_ok());
        assert!(conn.query("UPDATE tbl SET a = 'foo';").is_ok());
        assert!(conn.affected_rows == 2);
        for _ in &mut conn.query("SELECT * FROM tbl WHERE a = 'bar'") {
            assert!(false);
        }
        let mut count = 0;
        for row in &mut conn.query("SELECT * FROM tbl") {
            assert!(row.is_ok());
            let row = row.unwrap();
            if count == 0 {
                assert!(row[0] == Bytes((~"foo").into_bytes()));
                assert!(row[1] == Bytes((~"-123").into_bytes()));
                assert!(row[2] == Bytes((~"123").into_bytes()));
                assert!(row[3] == Bytes((~"2014-05-05").into_bytes()));
                assert!(row[4] == Bytes((~"123.123").into_bytes()));
            } else {
                assert!(row[0] == Bytes((~"foo").into_bytes()));
                assert!(row[1] == Bytes((~"-321").into_bytes()));
                assert!(row[2] == Bytes((~"321").into_bytes()));
                assert!(row[3] == Bytes((~"2014-06-06").into_bytes()));
                assert!(row[4] == Bytes((~"321.321").into_bytes()));
            }
            count += 1;
        }
        assert!(count == 2);
        for row in &mut conn.query("SELECT REPEAT('A', 20000000)") {
            assert!(row.is_ok());
            let row = row.unwrap();
            let val = row[0].bytes_ref();
            assert!(val.len() == 20000000);
            for y in val.iter() {
                assert!(y == &65u8);
            }
        }
    }

    #[test]
    fn test_prepared_statemenst() {
        let mut conn = MyConn::new(MyOpts{user: Some(~"root"),
                                          pass: Some(~"password"),
                                          ..Default::default()}).unwrap();
        assert!(conn.connect().is_ok());
        assert!(conn.query("DROP DATABASE IF EXISTS test").is_ok());
        assert!(conn.query("CREATE DATABASE test").is_ok());
        assert!(conn.query("USE test").is_ok());
        assert!(conn.query("CREATE TABLE tbl(a TEXT, b INT, c INT UNSIGNED, d DATE, e DOUBLE)").is_ok());
        let stmt = conn.prepare("INSERT INTO tbl(a, b, c, d, e) VALUES (?, ?, ?, ?, ?)");
        assert!(stmt.is_ok());
        let stmt = stmt.unwrap();
        assert!(conn.execute(&stmt, [Bytes((~"hello").into_bytes()), Int(-123), UInt(123), Date(2014, 5, 5,0,0,0,0), Float(123.123f64)]).is_ok());
        assert!(conn.execute(&stmt, [Bytes((~"world").into_bytes()), NULL, NULL, NULL, Float(321.321f64)]).is_ok());
        let stmt = conn.prepare("SELECT * FROM tbl");
        assert!(stmt.is_ok());
        let stmt = stmt.unwrap();
        let mut i = 0;
        for row in &mut conn.execute(&stmt, []) {
            assert!(row.is_ok());
            let row = row.unwrap();
            if i == 0 {
                assert!(row[0] == Bytes(~[104u8, 101u8, 108u8, 108u8, 111u8]));
                assert!(row[1] == Int(-123i64));
                assert!(row[2] == Int(123i64));
                assert!(row[3] == Date(2014u16, 5u8, 5u8, 0u8, 0u8, 0u8, 0u32));
                assert!(row[4].get_float().approx_eq(&123.123));
            } else {
                assert!(row[0] == Bytes(~[119u8, 111u8, 114u8, 108u8, 100u8]));
                assert!(row[1] == NULL);
                assert!(row[2] == NULL);
                assert!(row[3] == NULL);
                assert!(row[4].get_float().approx_eq(&321.321));
            }
            i += 1;
        }
        let stmt = conn.prepare("SELECT REPEAT('A', 20000000);");
        let stmt = stmt.unwrap();
        for row in &mut conn.execute(&stmt, []) {
            assert!(row.is_ok());
            let row = row.unwrap();
            let v: &[u8] = row[0].bytes_ref();
            assert!(v.len() == 20000000);
            for y in v.iter() {
                assert!(y == &65u8);
            }
        }
    }

    #[test]
    fn test_large_insert() {
        let mut conn = MyConn::new(MyOpts{user: Some(~"root"),
                                          pass: Some(~"password"),
                                          ..Default::default()}).unwrap();
        assert!(conn.connect().is_ok());
        assert!(conn.query("DROP DATABASE IF EXISTS test").is_ok());
        assert!(conn.query("CREATE DATABASE test").is_ok());
        assert!(conn.query("USE test").is_ok());
        assert!(conn.query("CREATE TABLE tbl(a LONGBLOB)").is_ok());
        let query = format!("INSERT INTO tbl(a) VALUES('{:s}')", str::from_chars(vec::from_elem(20000000, 'A')));
        conn.query(query);
        let x = (&mut conn.query("SELECT * FROM tbl")).next().unwrap();
        assert!(x.is_ok());
        let v: ~[u8] = x.unwrap()[0].unwrap_bytes();
        assert!(v.len() == 20000000);
        for y in v.iter() {
            assert!(y == &65u8);
        }

    }

    #[test]
    fn test_large_insert_prepared() {
        let mut conn = MyConn::new(MyOpts{user: Some(~"root"),
                                          pass: Some(~"password"),
                                          ..Default::default()}).unwrap();
        assert!(conn.connect().is_ok());
        assert!(conn.query("DROP DATABASE IF EXISTS test").is_ok());
        assert!(conn.query("CREATE DATABASE test").is_ok());
        assert!(conn.query("USE test").is_ok());
        assert!(conn.query("CREATE TABLE tbl(a LONGBLOB)").is_ok());
        let stmt = conn.prepare("INSERT INTO tbl(a) values ( ? );");
        assert!(stmt.is_ok());
        let stmt = stmt.unwrap();
        let val = vec::from_elem(20000000, 65u8);
        assert!(conn.execute(&stmt, [Bytes(val)]).is_ok());
        let row = (&mut conn.query("SELECT * FROM tbl")).next().unwrap();
        assert!(row.is_ok());
        let row = row.unwrap();
        let v = row[0].bytes_ref();
        assert!(v.len() == 20000000);
        for y in v.iter() {
            assert!(y == &65u8);
        }
    }

    #[test]
    fn test_local_infile() {
        let mut conn = MyConn::new(MyOpts{user: Some(~"root"),
                                          pass: Some(~"password"),
                                          ..Default::default()}).unwrap();
        assert!(conn.connect().is_ok());
        assert!(conn.query("DROP DATABASE IF EXISTS test").is_ok());
        assert!(conn.query("CREATE DATABASE test").is_ok());
        assert!(conn.query("USE test").is_ok());
        assert!(conn.query("CREATE TABLE tbl(a TEXT)").is_ok());
        let mut path = getcwd();
        path.push(~"local_infile.txt");
        {
            let mut file = File::create(&path).unwrap();
            file.write_line(&"AAAAAA");
            file.write_line(&"BBBBBB");
            file.write_line(&"CCCCCC");
        }
        let query = format!("LOAD DATA LOCAL INFILE '{:s}' INTO TABLE tbl", str::from_utf8_owned(path.clone().into_vec()));
        assert!(conn.query(query).is_ok());
        let mut count = 0;
        for row in &mut conn.query("SELECT * FROM tbl") {
            assert!(row.is_ok());
            let row = row.unwrap();
            match count {
                0 => assert!(row == ~[Bytes(~[65u8, 65u8, 65u8, 65u8, 65u8, 65u8])]),
                1 => assert!(row == ~[Bytes(~[66u8, 66u8, 66u8, 66u8, 66u8, 66u8])]),
                2 => assert!(row == ~[Bytes(~[67u8, 67u8, 67u8, 67u8, 67u8, 67u8])]),
                _ => assert!(false)
            }
            count += 1;
        }
        assert!(count == 3);
        unlink(&path);
    }

    #[bench]
    fn bench_simple_exec(bench: &mut extra::test::BenchHarness) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          ..Default::default()}).unwrap();
        assert!(conn.connect().is_ok());
        bench.iter(|| { conn.query("DO 1"); })
    }

    #[bench]
    fn bench_prepared_exec(bench: &mut extra::test::BenchHarness) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          ..Default::default()}).unwrap();
        assert!(conn.connect().is_ok());
        let stmt = conn.prepare("DO 1").unwrap();
        bench.iter(|| { conn.execute(&stmt, []); })
    }

    #[bench]
    fn bench_simple_query_row(bench: &mut extra::test::BenchHarness) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          ..Default::default()}).unwrap();
        assert!(conn.connect().is_ok());
        bench.iter(|| { (&mut conn.query("SELECT 1")).next(); })
    }

    #[bench]
    fn bench_simple_prepared_query_row(bench: &mut extra::test::BenchHarness) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          ..Default::default()}).unwrap();
        assert!(conn.connect().is_ok());
        let stmt = conn.prepare("SELECT 1").unwrap();
        bench.iter(|| { conn.execute(&stmt, []); })
    }

    #[bench]
    fn bench_simple_prepared_query_row_param(bench: &mut extra::test::BenchHarness) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          ..Default::default()}).unwrap();
        assert!(conn.connect().is_ok());
        let stmt = conn.prepare("SELECT ?").unwrap();
        let mut i = 0;
        bench.iter(|| { conn.execute(&stmt, [Int(i)]); i += 1; })
    }

    #[bench]
    fn bench_prepared_query_row_5param(bench: &mut extra::test::BenchHarness) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          ..Default::default()}).unwrap();
        assert!(conn.connect().is_ok());
        let stmt = conn.prepare("SELECT ?, ?, ?, ?, ?").unwrap();
        let params = ~[Int(42), Bytes(~[104u8, 101u8, 108u8, 108u8, 111u8, 111u8]), Float(1.618), NULL, Int(1)];
        bench.iter(|| { conn.execute(&stmt, params); })
    }

    #[bench]
    fn bench_select_large_string(bench: &mut extra::test::BenchHarness) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          ..Default::default()}).unwrap();
        assert!(conn.connect().is_ok());
        bench.iter(|| { for _ in &mut conn.query("SELECT REPEAT('A', 10000)") {} })
    }

    #[bench]
    fn bench_select_prepared_large_string(bench: &mut extra::test::BenchHarness) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          ..Default::default()}).unwrap();
        assert!(conn.connect().is_ok());
        let stmt = conn.prepare("SELECT REPEAT('A', 10000)").unwrap();
        bench.iter(|| { conn.execute(&stmt, []); })
    }
}
