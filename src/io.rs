use std::io;
use std::io::Read as NewRead;
use std::io::Write as NewWrite;
use std::net;

use super::value::Value;
use super::value::Value::{NULL, Int, UInt, Float, Bytes, Date, Time};
use super::consts;
use super::consts::Command;
use super::consts::ColumnType;
use super::error::MyError::MyDriverError;
use super::error::DriverError::PacketTooLarge;
use super::error::DriverError::PacketOutOfSync;
use super::error::MyResult;

#[cfg(feature = "openssl")]
use openssl::ssl;
use byteorder::ByteOrder;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use byteorder::LittleEndian as LE;
use unix_socket::UnixStream;

pub trait Read: ReadBytesExt {
    fn read_lenenc_int(&mut self) -> io::Result<u64> {
        let head_byte = try!(self.read_u8());
        let length = match head_byte {
            0xfc => 2,
            0xfd => 3,
            0xfe => 8,
            x => return Ok(x as u64),
        };
        let out = try!(self.read_uint::<LE>(length));
        Ok(out)
    }

    fn read_lenenc_bytes(&mut self) -> io::Result<Vec<u8>> {
        let len = try!(self.read_lenenc_int());
        let mut out = Vec::with_capacity(len as usize);
        if len > 0 {
            try!(self.take(len).read_to_end(&mut out));
        }
        Ok(out)
    }

    fn read_to_null(&mut self) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut chars = self.bytes();
        while let Some(c) = chars.next() {
            let c = try!(c);
            if c == 0u8 {
                break;
            }
            out.push(c);
        }
        Ok(out)
    }

    fn read_bin_value(&mut self, col_type: consts::ColumnType, unsigned: bool) -> io::Result<Value> {
        match col_type {
            ColumnType::MYSQL_TYPE_STRING |
            ColumnType::MYSQL_TYPE_VAR_STRING |
            ColumnType::MYSQL_TYPE_BLOB |
            ColumnType::MYSQL_TYPE_TINY_BLOB |
            ColumnType::MYSQL_TYPE_MEDIUM_BLOB |
            ColumnType::MYSQL_TYPE_LONG_BLOB |
            ColumnType::MYSQL_TYPE_SET |
            ColumnType::MYSQL_TYPE_ENUM |
            ColumnType::MYSQL_TYPE_DECIMAL |
            ColumnType::MYSQL_TYPE_VARCHAR |
            ColumnType::MYSQL_TYPE_BIT |
            ColumnType::MYSQL_TYPE_NEWDECIMAL |
            ColumnType::MYSQL_TYPE_GEOMETRY => {
                Ok(Bytes(try!(self.read_lenenc_bytes())))
            },
            ColumnType::MYSQL_TYPE_TINY => {
                if unsigned {
                    Ok(Int(try!(self.read_u8()) as i64))
                } else {
                    Ok(Int(try!(self.read_i8()) as i64))
                }
            },
            ColumnType::MYSQL_TYPE_SHORT |
            ColumnType::MYSQL_TYPE_YEAR => {
                if unsigned {
                    Ok(Int(try!(self.read_u16::<LE>()) as i64))
                } else {
                    Ok(Int(try!(self.read_i16::<LE>()) as i64))
                }
            },
            ColumnType::MYSQL_TYPE_LONG |
            ColumnType::MYSQL_TYPE_INT24 => {
                if unsigned {
                    Ok(Int(try!(self.read_u32::<LE>()) as i64))
                } else {
                    Ok(Int(try!(self.read_i32::<LE>()) as i64))
                }
            },
            ColumnType::MYSQL_TYPE_LONGLONG => {
                if unsigned {
                    Ok(UInt(try!(self.read_u64::<LE>())))
                } else {
                    Ok(Int(try!(self.read_i64::<LE>())))
                }
            },
            ColumnType::MYSQL_TYPE_FLOAT => {
                Ok(Float(try!(self.read_f32::<LE>()) as f64))
            },
            ColumnType::MYSQL_TYPE_DOUBLE => {
                Ok(Float(try!(self.read_f64::<LE>())))
            },
            ColumnType::MYSQL_TYPE_TIMESTAMP |
            ColumnType::MYSQL_TYPE_DATE |
            ColumnType::MYSQL_TYPE_DATETIME => {
                let len = try!(self.read_u8());
                let mut year = 0u16;
                let mut month = 0u8;
                let mut day = 0u8;
                let mut hour = 0u8;
                let mut minute = 0u8;
                let mut second = 0u8;
                let mut micro_second = 0u32;
                if len >= 4u8 {
                    year = try!(self.read_u16::<LE>());
                    month = try!(self.read_u8());
                    day = try!(self.read_u8());
                }
                if len >= 7u8 {
                    hour = try!(self.read_u8());
                    minute = try!(self.read_u8());
                    second = try!(self.read_u8());
                }
                if len == 11u8 {
                    micro_second = try!(self.read_u32::<LE>());
                }
                Ok(Date(year, month, day, hour, minute, second, micro_second))
            },
            ColumnType::MYSQL_TYPE_TIME => {
                let len = try!(self.read_u8());
                let mut is_negative = false;
                let mut days = 0u32;
                let mut hours = 0u8;
                let mut minutes = 0u8;
                let mut seconds = 0u8;
                let mut micro_seconds = 0u32;
                if len >= 8u8 {
                    is_negative = try!(self.read_u8()) == 1u8;
                    days = try!(self.read_u32::<LE>());
                    hours = try!(self.read_u8());
                    minutes = try!(self.read_u8());
                    seconds = try!(self.read_u8());
                }
                if len == 12u8 {
                    micro_seconds = try!(self.read_u32::<LE>());
                }
                Ok(Time(is_negative, days, hours, minutes, seconds, micro_seconds))
            },
            _ => Ok(NULL),
        }
    }

    /// Reads mysql packet payload returns it with new seq_id value.
    fn read_packet(&mut self, mut seq_id: u8) -> MyResult<(Vec<u8>, u8)> {
        let mut output = Vec::new();
        let mut pos = 0;
        loop {
            let payload_len = try!(self.read_uint::<LE>(3)) as usize;
            let srv_seq_id = try!(self.read_u8());
            if srv_seq_id != seq_id {
                return Err(MyDriverError(PacketOutOfSync));
            }
            seq_id += 1;
            if payload_len == consts::MAX_PAYLOAD_LEN {
                output.reserve(pos + consts::MAX_PAYLOAD_LEN);
                unsafe { output.set_len(pos + consts::MAX_PAYLOAD_LEN); }
                let mut chunk = self.take(consts::MAX_PAYLOAD_LEN as u64);
                loop {
                    let count = try!(chunk.read(&mut output[pos..]));
                    if count == 0 {
                        break;
                    } else {
                        pos += count;
                    }
                }
            } else if payload_len == 0 {
                break;
            } else {
                output.reserve(pos + payload_len);
                unsafe { output.set_len(pos + payload_len); }
                let mut chunk = self.take(payload_len as u64);
                loop {
                    let count = try!(chunk.read(&mut output[pos..]));
                    if count == 0 {
                        break;
                    } else {
                        pos += count;
                    }
                }
                break;
            }
        }
        Ok((output, seq_id))
    }
}

impl<T: ReadBytesExt> Read for T {}

pub trait Write: WriteBytesExt {
    fn write_le_uint_n(&mut self, x: u64, len: usize) -> io::Result<()> {
        let mut buf = vec![0u8; len];
        let mut offset = 0;
        while offset < len {
            buf[offset] = (((0xFF << (offset * 8)) & x) >> (offset * 8)) as u8;
            offset += 1;
        }
        NewWrite::write_all(self, &buf[..])
    }

    fn write_lenenc_int(&mut self, x: u64) -> io::Result<()> {
        if x < 251 {
            self.write_le_uint_n(x, 1)
        } else if x < 65_536 {
            try!(self.write_u8(0xFC));
            self.write_le_uint_n(x, 2)
        } else if x < 16_777_216 {
            try!(self.write_u8(0xFD));
            self.write_le_uint_n(x, 3)
        } else {
            try!(self.write_u8(0xFE));
            self.write_le_uint_n(x, 8)
        }
    }

    fn write_lenenc_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        try!(self.write_lenenc_int(bytes.len() as u64));
        NewWrite::write_all(self, bytes)
    }

    fn write_packet(&mut self, data: &[u8], mut seq_id: u8, max_allowed_packet: usize) -> MyResult<u8> {
        if data.len() > max_allowed_packet &&
           max_allowed_packet < consts::MAX_PAYLOAD_LEN {
            return Err(MyDriverError(PacketTooLarge));
        }
        if data.len() == 0 {
            try!(NewWrite::write_all(self, &[0u8, 0u8, 0u8, seq_id]));
            return Ok(seq_id + 1);
        }
        let mut last_was_max = false;
        for chunk in data.chunks(consts::MAX_PAYLOAD_LEN) {
            let chunk_len = chunk.len();
            let mut writer = Vec::with_capacity(4 + chunk_len);
            last_was_max = chunk_len == consts::MAX_PAYLOAD_LEN;
            try!(writer.write_le_uint_n(chunk_len as u64, 3));
            try!(WriteBytesExt::write_u8(&mut writer, seq_id));
            seq_id += 1;
            try!(NewWrite::write_all(&mut writer, chunk));
            try!(self.write_all(&writer[..]));
        }
        if last_was_max {
            try!(NewWrite::write_all(self, &[0u8, 0u8, 0u8, seq_id]));
            seq_id += 1;
        }
        Ok(seq_id)
    }
}

impl<T: WriteBytesExt> Write for T {}

#[cfg(feature = "openssl")]
pub struct MySslStream(pub ssl::SslStream<PlainStream>);

#[cfg(feature = "openssl")]
impl Drop for MySslStream {
    fn drop(&mut self) {
        let MySslStream(ref mut s) = *self;
        let _ = s.write_packet(&[Command::COM_QUIT as u8], 0, consts::MAX_PAYLOAD_LEN);
        let _ = s.flush();
    }
}

pub enum MyStream {
    #[cfg(feature = "openssl")]
    SecureStream(MySslStream),
    InsecureStream(PlainStream),
}

#[cfg(feature = "ssl")]
impl io::Read for MyStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            MyStream::SecureStream(MySslStream(ref mut s)) => s.read(buf),
            MyStream::InsecureStream(ref mut s) => s.read(buf),
        }
    }
}

#[cfg(not(feature = "ssl"))]
impl io::Read for MyStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            MyStream::InsecureStream(ref mut s) => s.read(buf),
        }
    }
}

#[cfg(feature = "ssl")]
impl io::Write for MyStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            MyStream::SecureStream(MySslStream(ref mut s)) => NewWrite::write(s, buf),
            MyStream::InsecureStream(ref mut s) => NewWrite::write(s, buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            MyStream::SecureStream(MySslStream(ref mut s)) => s.flush(),
            MyStream::InsecureStream(ref mut s) => s.flush(),
        }
    }
}

#[cfg(not(feature = "ssl"))]
impl io::Write for MyStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            MyStream::InsecureStream(ref mut s) => NewWrite::write(s, buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            MyStream::InsecureStream(ref mut s) => s.flush(),
        }
    }
}

pub enum TcpOrUnixStream {
    TCPStream(net::TcpStream),
    UNIXStream(UnixStream),
}

pub struct PlainStream {
    pub s: TcpOrUnixStream,
    pub wrapped: bool,
}

impl Drop for PlainStream {
    fn drop(&mut self) {
        if ! self.wrapped {
            let _ = self.write_packet(&[Command::COM_QUIT as u8], 0, consts::MAX_PAYLOAD_LEN);
            let _ = self.flush();
        }
    }
}

impl io::Read for PlainStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.s {
            TcpOrUnixStream::TCPStream(ref mut s) => io::Read::read(s, buf),
            TcpOrUnixStream::UNIXStream(ref mut s) => io::Read::read(s, buf),
        }
    }
}

impl io::Write for PlainStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.s {
            TcpOrUnixStream::TCPStream(ref mut s) => NewWrite::write(s, buf),
            TcpOrUnixStream::UNIXStream(ref mut s) => NewWrite::write(s, buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.s {
            TcpOrUnixStream::TCPStream(ref mut s) => s.flush(),
            TcpOrUnixStream::UNIXStream(ref mut s) => s.flush(),
        }
    }
}
