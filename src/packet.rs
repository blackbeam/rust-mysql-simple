use std::io;
use std::io::Read as StdRead;

use regex::Regex;
use byteorder::LittleEndian as LE;
use byteorder::{ReadBytesExt};

use super::consts;
use super::consts::StatusFlags;
use super::consts::CapabilityFlags;
use super::error;
use super::error::DriverError;
use super::io::Read;

lazy_static! {
    static ref VERSION_RE: Regex = {
        Regex::new(r"^(\d{1,2})\.(\d{1,2})\.(\d{1,3})(.*)").unwrap()
    };
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct OkPacket {
    pub affected_rows: u64,
    pub last_insert_id: u64,
    pub status_flags: StatusFlags,
    pub warnings: u16,
    pub info: Vec<u8>
}

impl OkPacket {
    pub fn from_payload(pld: &[u8]) -> io::Result<OkPacket> {
        let mut reader = &pld[1..];
        Ok(OkPacket{
            affected_rows: try!(reader.read_lenenc_int()),
            last_insert_id: try!(reader.read_lenenc_int()),
            status_flags: StatusFlags::from_bits_truncate(try!(reader.read_u16::<LE>())),
            warnings: try!(reader.read_u16::<LE>()),
            info: {
                let mut info = Vec::with_capacity(reader.len());
                try!(reader.read_to_end(&mut info));
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
    pub fn from_payload(pld: &[u8], c_flags: consts::CapabilityFlags) -> io::Result<ErrPacket> {
        let mut reader = &pld[1..];
        let error_code = try!(reader.read_u16::<LE>());
        Ok(ErrPacket{
            error_code: error_code,
            sql_state: {
                if c_flags.contains(consts::CLIENT_PROTOCOL_41) {
                    try!(reader.read_u8());
                    let mut sql_state = Vec::with_capacity(5);
                    try!(reader.by_ref().take(5).read_to_end(&mut sql_state));
                    sql_state
                } else {
                    b"NY000".to_vec()
                }
            },
            error_message: {
                let mut error_message = Vec::with_capacity(reader.len());
                try!(reader.read_to_end(&mut error_message));
                error_message
            },
        })
    }
}

impl From<ErrPacket> for error::MySqlError {
    fn from(x: ErrPacket) -> error::MySqlError {
        let ErrPacket {sql_state, error_message, error_code} = x;
        error::MySqlError {
            state: String::from_utf8_lossy(&*sql_state).into_owned(),
            code: error_code,
            message: String::from_utf8_lossy(&*error_message).into_owned(),
        }
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct EOFPacket {
    pub warnings: u16,
    pub status_flags: StatusFlags,
}

impl EOFPacket {
    pub fn from_payload(pld: &[u8]) -> io::Result<EOFPacket> {
        let mut reader = &pld[1..];
        Ok(EOFPacket{
            warnings: try!(reader.read_u16::<LE>()),
            status_flags: StatusFlags::from_bits_truncate(try!(reader.read_u16::<LE>())),
        })
    }
}

/// (major, minor, micro) mysql server version.
pub type ServerVersion = (u16, u16, u16);

fn parse_version(bytes: &[u8]) -> error::Result<ServerVersion> {
    let ver_str = String::from_utf8_lossy(bytes).into_owned();
    VERSION_RE.captures(&ver_str[..])
    .and_then(|capts| {
        Some((
            (capts.at(1).unwrap().parse::<u16>()).unwrap_or(0),
            (capts.at(2).unwrap().parse::<u16>()).unwrap_or(0),
            (capts.at(3).unwrap().parse::<u16>()).unwrap_or(0),
        ))
    }).and_then(|version| {
        if version == (0, 0, 0) {
            None
        } else {
            Some(version)
        }
    }).ok_or(DriverError::CouldNotParseVersion.into())
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct HandshakePacket {
    pub auth_plugin_data: Vec<u8>,
    pub auth_plugin_name: Vec<u8>,
    pub server_version: ServerVersion,
    pub connection_id: u32,
    pub capability_flags: CapabilityFlags,
    pub status_flags: StatusFlags,
    pub protocol_version: u8,
    pub character_set: u8,
}

impl HandshakePacket {
    pub fn from_payload(pld: &[u8]) -> error::Result<HandshakePacket> {
        let mut length_of_auth_plugin_data = 0i16;
        let mut auth_plugin_data: Vec<u8> = Vec::with_capacity(32);
        let mut auth_plugin_name: Vec<u8> = Vec::with_capacity(32);
        let mut character_set = 0u8;
        let mut status_flags = StatusFlags::empty();
        let mut reader = &pld[..];
        let protocol_version = try!(reader.read_u8());
        let version_bytes = try!(reader.read_to_null());
        let server_version = try!(parse_version(&version_bytes[..]));
        let connection_id = try!(reader.read_u32::<LE>());
        auth_plugin_data.resize(8, 0);
        try!(reader.read_exact(&mut *auth_plugin_data));
        // skip filler
        reader = &reader[1..];
        let lower_cf = try!(reader.read_u16::<LE>());
        let mut capability_flags = CapabilityFlags::from_bits_truncate(lower_cf as u32);
        if reader.len() > 0 {
            character_set = try!(reader.read_u8());
            status_flags = StatusFlags::from_bits_truncate(try!(reader.read_u16::<LE>()));
            let upper_cf = try!(reader.read_u16::<LE>());
            capability_flags.insert(CapabilityFlags::from_bits_truncate((upper_cf as u32) << 16));
            if capability_flags.contains(consts::CLIENT_PLUGIN_AUTH) {
                length_of_auth_plugin_data = try!(reader.read_u8()) as i16;
            } else {
                reader = &reader[1..];
            }
            reader = &reader[10..];
            if capability_flags.contains(consts::CLIENT_SECURE_CONNECTION) {
                let mut len = length_of_auth_plugin_data - 8i16;
                len = if len > 13i16 { len } else { 13i16 };
                auth_plugin_data.reserve_exact(len as usize);
                auth_plugin_data.resize(len as usize + 8, 0);
                try!(reader.read_exact(&mut auth_plugin_data[8..]));
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
    pub fn get_default_collation(&self) -> u8 {
        // MySQL version 5.5.3 introduced "utf8mb4"
        if self.server_version >= (5, 5, 3) {
            // This is a true UTF-8 collation, which supports the full range of code-points
            consts::UTF8MB4_GENERAL_CI
        } else {
            // Fall-back to the "utf8" collation which only supports up to 3-byte code-points
            consts::UTF8_GENERAL_CI
        }
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
        let err_packet = ErrPacket::from_payload(&payload, consts::CLIENT_PROTOCOL_41).unwrap();
        assert_eq!(err_packet.error_code, 1);
        assert_eq!(err_packet.sql_state, vec!(51u8, 68u8, 48u8, 48u8, 48u8));
        assert_eq!(err_packet.error_message, vec!(32u8, 32u8));
    }
    #[test]
    fn should_parse_EOF_packet() {
        let payload = [0xfe_u8, 1u8, 0u8, 8u8, 0u8];
        let eof_packet = EOFPacket::from_payload(&payload).unwrap();
        assert_eq!(eof_packet.warnings, 1);
        assert_eq!(eof_packet.status_flags, consts::SERVER_MORE_RESULTS_EXISTS);
    }
    #[test]
    fn should_parse_handshake_packet() {
        let payload = b"\x0a5.6.4\x00\x01\x00\x00\x00\x01\x02\x03\x04\x05\
                        \x06\x07\x08\x00\x04\x80";
        let handshake_packet = HandshakePacket::from_payload(payload).unwrap();
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
