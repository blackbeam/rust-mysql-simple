use std::{fmt, str};
use std::io::{IoResult, BufReader, SeekCur};
use super::io::{MyReader};
use super::consts;

#[deriving(PartialEq)]
pub struct OkPacket {
    pub affected_rows: u64,
    pub last_insert_id: u64,
    pub status_flags: u16,
    pub warnings: u16,
    pub info: Vec<u8>
}

impl OkPacket {
    pub fn from_payload(pld: &[u8]) -> IoResult<OkPacket> {
        let mut reader = BufReader::new(pld);
        try!(reader.seek(1, SeekCur));
        Ok(OkPacket{
            affected_rows: try!(reader.read_lenenc_int()),
            last_insert_id: try!(reader.read_lenenc_int()),
            status_flags: try!(reader.read_le_u16()),
            warnings: try!(reader.read_le_u16()),
            info: try!(reader.read_to_end())
        })
    }
}

#[deriving(PartialEq)]
pub struct ErrPacket {
    pub sql_state: Vec<u8>,
    pub error_message: Vec<u8>,
    pub error_code: u16
}

impl ErrPacket {
    pub fn from_payload(pld: &[u8]) -> IoResult<ErrPacket> {
        let mut reader = BufReader::new(pld);
        try!(reader.seek(1, SeekCur));
        let error_code = try!(reader.read_le_u16());
        try!(reader.seek(1, SeekCur));
        Ok(ErrPacket{
            error_code: error_code,
            sql_state: try!(reader.read_exact(5)),
            error_message: try!(reader.read_to_end())
        })
    }
}

impl fmt::Show for ErrPacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "ERROR {:u} ({:s}): {:s}",
               self.error_code,
               str::from_utf8(self.sql_state.as_slice()).unwrap(),
               str::from_utf8(self.error_message.as_slice()).unwrap())
    }
}

#[deriving(PartialEq)]
pub struct EOFPacket {
    pub warnings: u16,
    pub status_flags: u16
}

impl EOFPacket {
    pub fn from_payload(pld: &[u8]) -> IoResult<EOFPacket> {
        let mut reader = BufReader::new(pld);
        try!(reader.seek(1, SeekCur));
        Ok(EOFPacket{
            warnings: try!(reader.read_le_u16()),
            status_flags: try!(reader.read_le_u16())
        })
    }
}

pub struct HandshakePacket {
    pub auth_plugin_data: Vec<u8>,
    pub auth_plugin_name: Vec<u8>,
    pub connection_id: u32,
    pub capability_flags: u32,
    pub status_flags: u16,
    pub protocol_version: u8,
    pub character_set: u8,
}

impl HandshakePacket {
    pub fn from_payload(pld: &[u8]) -> IoResult<HandshakePacket> {
        let mut length_of_auth_plugin_data = 0i16;
        let mut auth_plugin_data: Vec<u8> = Vec::with_capacity(32);
        let mut auth_plugin_name: Vec<u8> = Vec::with_capacity(32);
        let mut character_set = 0u8;
        let mut status_flags = 0u16;
        let payload_len = pld.len();
        let mut reader = BufReader::new(pld);
        let protocol_version = try!(reader.read_u8());
        // skip server version
        while try!(reader.read_u8()) != 0u8 {}
        let connection_id = try!(reader.read_le_u32());
        try!(reader.push(8, &mut auth_plugin_data));
        // skip filler
        try!(reader.seek(1, SeekCur));
        let mut capability_flags = try!(reader.read_le_u16()) as u32;
        if try!(reader.tell()) != payload_len as u64 {
            character_set = try!(reader.read_u8());
            status_flags = try!(reader.read_le_u16());
            capability_flags |= (try!(reader.read_le_u16()) as u32) << 16;
            if (capability_flags & consts::CLIENT_PLUGIN_AUTH) > 0 {
                length_of_auth_plugin_data = try!(reader.read_u8()) as i16;
            } else {
                try!(reader.seek(1, SeekCur));
            }
            try!(reader.seek(10, SeekCur));
            if (capability_flags & consts::CLIENT_SECURE_CONNECTION) > 0 {
                let mut len = length_of_auth_plugin_data - 8i16;
                len = if len > 13i16 { len } else { 13i16 };
                try!(reader.push(len as uint, &mut auth_plugin_data));
                if *auth_plugin_data.get(auth_plugin_data.len() - 1) == 0u8 {
                    auth_plugin_data.pop();
                }
            }
            if (capability_flags & consts::CLIENT_PLUGIN_AUTH) > 0 {
                auth_plugin_name = try!(reader.read_to_end());
                if *auth_plugin_name.get(auth_plugin_name.len() - 1) == 0u8 {
                    auth_plugin_name.pop();
                }
            }
        }
        Ok(HandshakePacket{protocol_version: protocol_version, connection_id: connection_id,
                         auth_plugin_data: auth_plugin_data,
                         capability_flags: capability_flags, character_set: character_set,
                         status_flags: status_flags, auth_plugin_name: auth_plugin_name})
    }
}

#[cfg(test)]
mod test {
    use super::{OkPacket, ErrPacket, EOFPacket, HandshakePacket};

    #[test]
    fn test_ok_packet() {
        let payload = [0u8, 1u8, 2u8, 3u8, 0u8, 4u8, 0u8, 32u8];
        let ok_packet = OkPacket::from_payload(payload);
        assert!(ok_packet.is_ok());
        let ok_packet = ok_packet.unwrap();
        assert!(ok_packet.affected_rows == 1);
        assert!(ok_packet.last_insert_id == 2);
        assert!(ok_packet.status_flags == 3);
        assert!(ok_packet.warnings == 4);
        assert!(ok_packet.info == vec!(32u8));
    }

    #[test]
    fn test_err_packet() {
        let payload = [255u8, 1u8, 0u8, 35u8, 51u8, 68u8, 48u8, 48u8, 48u8, 32u8, 32u8];
        let err_packet = ErrPacket::from_payload(payload);
        assert!(err_packet.is_ok());
        let err_packet = err_packet.unwrap();
        assert!(err_packet.error_code == 1);
        assert!(err_packet.sql_state == vec!(51u8, 68u8, 48u8, 48u8, 48u8));
        assert!(err_packet.error_message == vec!(32u8, 32u8));
    }

    #[test]
    fn test_eof_packet() {
        let payload = [0xfe_u8, 1u8, 0u8, 2u8, 0u8];
        let eof_packet = EOFPacket::from_payload(payload);
        assert!(eof_packet.is_ok());
        let eof_packet = eof_packet.unwrap();
        assert!(eof_packet.warnings == 1);
        assert!(eof_packet.status_flags == 2);
    }

    #[test]
    fn test_handshake_packet() {
        let payload =  [0x0a_u8,
                        32u8, 32u8, 32u8, 32u8, 0u8,
                        1u8, 0u8, 0u8, 0u8,
                        1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8,
                        0u8,
                        3u8, 0x80_u8];
        let handshake_packet = HandshakePacket::from_payload(payload);
        assert!(handshake_packet.is_ok());
        let handshake_packet = handshake_packet.unwrap();
        assert!(handshake_packet.protocol_version == 0x0a);
        assert!(handshake_packet.connection_id == 1);
        assert!(handshake_packet.auth_plugin_data == vec!(1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8));
        assert!(handshake_packet.capability_flags == 0x00008003);
        let mut payload = Vec::from_slice(payload);
        payload.push(33u8);
        payload.push_all_move(vec!(4u8, 0u8));
        payload.push_all_move(vec!(0x08_u8, 0u8));
        payload.push_all_move(vec!(0x15_u8));
        payload.push_all_move(::std::vec::Vec::from_elem(10, 0u8));
        payload.push_all_move(vec!(0x26_u8, 0x3a_u8, 0x34_u8, 0x34_u8, 0x46_u8, 0x44_u8,
                                0x63_u8, 0x44_u8, 0x69_u8, 0x63_u8, 0x39_u8, 0x30_u8, 0x00_u8));
        payload.push_all_move(vec!(1u8, 2u8, 3u8, 4u8, 5u8, 0u8));
        let handshake_packet = HandshakePacket::from_payload(payload.as_slice());
        assert!(handshake_packet.is_ok());
        let handshake_packet = handshake_packet.unwrap();
        assert!(handshake_packet.protocol_version == 0x0a);
        assert!(handshake_packet.connection_id == 1);
        assert!(handshake_packet.auth_plugin_data == vec!(1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8,
                                                       0x26_u8, 0x3a_u8, 0x34_u8, 0x34_u8, 0x46_u8, 0x44_u8,
                                                       0x63_u8, 0x44_u8, 0x69_u8, 0x63_u8, 0x39_u8, 0x30_u8));
        assert!(handshake_packet.capability_flags == 0x00088003);
        assert!(handshake_packet.character_set == 33);
        assert!(handshake_packet.status_flags == 4);
        assert!(handshake_packet.auth_plugin_name == vec!(1u8, 2u8, 3u8, 4u8, 5u8));
    }
}
