use std::{fmt, str};
use std::io;
use std::io::Seek;
use std::io::Read as NewRead;

use regex::Regex;
use byteorder::LittleEndian as LE;
use byteorder::{ByteOrder, ReadBytesExt};

use super::io::{Read};
use super::consts;
use super::error;
use super::error::DriverError;
use super::error::MyError::MyDriverError;

#[derive(Clone, Eq, PartialEq)]
pub struct OkPacket {
    pub affected_rows: u64,
    pub last_insert_id: u64,
    pub status_flags: consts::StatusFlags,
    pub warnings: u16,
    pub info: Vec<u8>
}

impl OkPacket {
    pub fn from_payload(pld: &[u8]) -> io::Result<OkPacket> {
        let mut reader = &pld[1..];
        Ok(OkPacket{
            affected_rows: try!(reader.read_lenenc_int()),
            last_insert_id: try!(reader.read_lenenc_int()),
            status_flags: consts::StatusFlags::from_bits_truncate(try!(reader.read_u16::<LE>())),
            warnings: try!(reader.read_u16::<LE>()),
            info: {
                let mut info = Vec::with_capacity(reader.len());
                try!(NewRead::read_to_end(&mut reader, &mut info));
                info
            },
        })
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ErrPacket {
    pub sql_state: Vec<u8>,
    pub error_message: Vec<u8>,
    pub error_code: u16
}

impl ErrPacket {
    pub fn from_payload(pld: &[u8]) -> io::Result<ErrPacket> {
        let mut reader = &pld[1..];
        let error_code = try!(reader.read_u16::<LE>());
        try!(ReadBytesExt::read_u8(&mut reader));
        Ok(ErrPacket{
            error_code: error_code,
            sql_state: {
                let mut sql_state = Vec::with_capacity(5);
                try!(reader.by_ref().take(5).read_to_end(&mut sql_state));
                sql_state
            },
            error_message: {
                let mut error_message = Vec::with_capacity(reader.len());
                try!(NewRead::read_to_end(&mut reader, &mut error_message));
                error_message
            },
        })
    }
}

impl fmt::Display for ErrPacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "ERROR {} ({}): {}",
               self.error_code,
               str::from_utf8(&self.sql_state[..]).unwrap(),
               str::from_utf8(&self.error_message[..]).unwrap())
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct EOFPacket {
    pub warnings: u16,
    pub status_flags: consts::StatusFlags,
}

impl EOFPacket {
    pub fn from_payload(pld: &[u8]) -> io::Result<EOFPacket> {
        let mut reader = &pld[1..];
        Ok(EOFPacket{
            warnings: try!(reader.read_u16::<LE>()),
            status_flags: consts::StatusFlags::from_bits_truncate(try!(reader.read_u16::<LE>())),
        })
    }
}

/// (major, minor, micro) mysql server version.
pub type ServerVersion = (u16, u16, u16);
static VERSION_RE: Regex = regex!(r"^(\d{1,2})\.(\d{1,2})\.(\d{1,3})(.*)");

fn parse_version(bytes: &[u8]) -> error::MyResult<ServerVersion> {
    let ver_str = String::from_utf8_lossy(bytes).into_owned();
    VERSION_RE.captures(&ver_str[..])
    .and_then(|capts| {
        Some((
            (capts.at(1).unwrap().parse::<u16>()).unwrap_or(0),
            (capts.at(2).unwrap().parse::<u16>()).unwrap_or(0),
            (capts.at(3).unwrap().parse::<u16>()).unwrap_or(0),
        ))
    })
    .and_then(|version| {
        if version == (0, 0, 0) {
            None
        } else {
            Some(version)
        }
    }).ok_or(MyDriverError(DriverError::CouldNotParseVersion))
}

#[derive(Clone, Eq, PartialEq)]
pub struct HandshakePacket {
    pub auth_plugin_data: Vec<u8>,
    pub auth_plugin_name: Vec<u8>,
    pub server_version: ServerVersion,
    pub connection_id: u32,
    pub capability_flags: consts::CapabilityFlags,
    pub status_flags: consts::StatusFlags,
    pub protocol_version: u8,
    pub character_set: u8,
}

impl HandshakePacket {
    pub fn from_payload(pld: &[u8]) -> error::MyResult<HandshakePacket> {
        let mut length_of_auth_plugin_data = 0i16;
        let mut auth_plugin_data: Vec<u8> = Vec::with_capacity(32);
        let mut auth_plugin_name: Vec<u8> = Vec::with_capacity(32);
        let mut character_set = 0u8;
        let mut status_flags = consts::StatusFlags::empty();
        let payload_len = pld.len();
        let mut reader = io::Cursor::new(pld);
        let protocol_version = try!(ReadBytesExt::read_u8(&mut reader));
        let version_bytes = try!(reader.read_to_null());
        let server_version = try!(parse_version(&version_bytes[..]));
        let connection_id = try!(reader.read_u32::<LE>());
        try!(reader.by_ref().take(8).read_to_end(&mut auth_plugin_data));
        // skip filler
        try!(reader.seek(io::SeekFrom::Current(1)));
        let lower_cf = try!(reader.read_u16::<LE>());
        let mut capability_flags = consts::CapabilityFlags::from_bits_truncate(lower_cf as u32);
        if reader.position() != payload_len as u64 {
            character_set = try!(ReadBytesExt::read_u8(&mut reader));
            status_flags = consts::StatusFlags::from_bits_truncate(try!(reader.read_u16::<LE>()));
            let upper_cf = try!(reader.read_u16::<LE>());
            capability_flags.insert(consts::CapabilityFlags::from_bits_truncate((upper_cf as u32) << 16));
            if capability_flags.contains(consts::CLIENT_PLUGIN_AUTH) {
                length_of_auth_plugin_data = try!(ReadBytesExt::read_u8(&mut reader)) as i16;
            } else {
                try!(reader.seek(io::SeekFrom::Current(1)));
            }
            try!(reader.seek(io::SeekFrom::Current(10)));
            if capability_flags.contains(consts::CLIENT_SECURE_CONNECTION) {
                let mut len = length_of_auth_plugin_data - 8i16;
                len = if len > 13i16 { len } else { 13i16 };
                try!(reader.by_ref().take(len as u64).read_to_end(&mut auth_plugin_data));
                if auth_plugin_data[auth_plugin_data.len() - 1] == 0u8 {
                    auth_plugin_data.pop();
                }
            }
            if capability_flags.contains(consts::CLIENT_PLUGIN_AUTH) {
                try!(reader.read_to_end(&mut auth_plugin_name));
                if auth_plugin_name[auth_plugin_name.len() - 1] == 0u8 {
                    auth_plugin_name.pop();
                }
            }
        }
        Ok(HandshakePacket{protocol_version: protocol_version, connection_id: connection_id,
                         auth_plugin_data: auth_plugin_data, server_version: server_version,
                         capability_flags: capability_flags, character_set: character_set,
                         status_flags: status_flags, auth_plugin_name: auth_plugin_name})
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod test {
    pub use std::iter;
    pub use super::super::consts;
    pub use super::{OkPacket, ErrPacket, EOFPacket, HandshakePacket};

    #[test]
    fn should_parse_OK_packet() {
        let payload = [0u8, 1u8, 2u8, 8u8, 0u8, 4u8, 0u8, 32u8];
        let ok_packet = OkPacket::from_payload(&payload).unwrap();
        assert_eq!(ok_packet.affected_rows, 1);
        assert_eq!(ok_packet.last_insert_id, 2);
        assert_eq!(ok_packet.status_flags, consts::SERVER_MORE_RESULTS_EXISTS);
        assert_eq!(ok_packet.warnings, 4);
        assert_eq!(ok_packet.info, vec!(32u8));
    }
    #[test]
    fn should_parse_Error_packet() {
        let payload = [255u8, 1u8, 0u8, 35u8, 51u8, 68u8, 48u8, 48u8, 48u8,
                       32u8, 32u8];
        let err_packet = ErrPacket::from_payload(&payload).unwrap();
        assert_eq!(err_packet.error_code, 1);
        assert_eq!(err_packet.sql_state, vec!(51u8, 68u8, 48u8, 48u8, 48u8));
        assert_eq!(err_packet.error_message, vec!(32u8, 32u8));
    }
    #[test]
    fn should_parse_EOF_packet() {
        let payload = [0xfe_u8, 1u8, 0u8, 8u8, 0u8];
        let eof_packet = EOFPacket::from_payload(&payload).unwrap();
        assert_eq!(eof_packet.warnings, 1);
        assert_eq!(eof_packet.status_flags,
                   consts::SERVER_MORE_RESULTS_EXISTS);
    }
    #[test]
    fn should_parse_handshake_packet() {
        let payload = b"\x0a5.6.4\x00\x01\x00\x00\x00\x01\x02\x03\x04\x05\
                        \x06\x07\x08\x00\x04\x80";
        let handshake_packet = HandshakePacket::from_payload(payload)
                                               .unwrap();
        assert_eq!(handshake_packet.protocol_version, 0x0a);
        assert_eq!(handshake_packet.server_version, (5, 6, 4));
        assert_eq!(handshake_packet.connection_id, 1);
        assert_eq!(handshake_packet.auth_plugin_data,
                   vec!(1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8));
        assert_eq!(handshake_packet.capability_flags,
            consts::CLIENT_SECURE_CONNECTION | consts::CLIENT_LONG_FLAG);
        let mut payload = payload.to_vec();
        payload.push(33u8);
        payload.extend(vec!(8u8, 0u8).into_iter());
        payload.extend(vec!(0x08_u8, 0u8).into_iter());
        payload.extend(vec!(0x15_u8).into_iter());
        payload.extend(iter::repeat(0u8).take(10));
        payload.extend(vec!(0x26_u8, 0x3a_u8, 0x34_u8, 0x34_u8, 0x46_u8,
                            0x44_u8, 0x63_u8, 0x44_u8, 0x69_u8, 0x63_u8,
                            0x39_u8, 0x30_u8, 0x00_u8).into_iter());
        payload.extend(vec!(1u8, 2u8, 3u8, 4u8, 5u8, 0u8).into_iter());
        let handshake_packet =
            HandshakePacket::from_payload(&payload[..]).unwrap();
        assert_eq!(handshake_packet.protocol_version, 0x0a);
        assert_eq!(handshake_packet.connection_id, 1);
        assert_eq!(
            handshake_packet.auth_plugin_data,
            vec!(1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8,
                 0x26_u8, 0x3a_u8, 0x34_u8, 0x34_u8, 0x46_u8, 0x44_u8,
                 0x63_u8, 0x44_u8, 0x69_u8, 0x63_u8, 0x39_u8, 0x30_u8)
        );
        assert_eq!(handshake_packet.capability_flags,
                   consts::CLIENT_SECURE_CONNECTION |
                   consts::CLIENT_PLUGIN_AUTH |
                   consts::CLIENT_LONG_FLAG);
        assert_eq!(handshake_packet.character_set, 33);
        assert_eq!(handshake_packet.status_flags,
                   consts::SERVER_MORE_RESULTS_EXISTS);
        assert_eq!(handshake_packet.auth_plugin_name,
                   vec!(1u8, 2u8, 3u8, 4u8, 5u8));
    }
}
