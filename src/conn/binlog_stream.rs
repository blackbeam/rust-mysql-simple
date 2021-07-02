// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use mysql_common::{
    binlog::{
        consts::BinlogVersion::Version4,
        events::{Event, TableMapEvent},
        EventStreamReader,
    },
    io::ParseBuf,
    packets::{ErrPacket, NetworkStreamTerminator, OkPacketDeserializer},
};

use crate::Conn;

/// Binlog event stream.
///
/// Stream initialization is lazy, i.e. binlog won't be requested until this stream is polled.
pub struct BinlogStream {
    conn: Option<Conn>,
    esr: EventStreamReader,
}

impl BinlogStream {
    /// `conn` is a `Conn` with `request_binlog` executed on it.
    pub(super) fn new(conn: Conn) -> Self {
        BinlogStream {
            conn: Some(conn),
            esr: EventStreamReader::new(Version4),
        }
    }

    /// Returns a table map event for the given table id.
    pub fn get_tme(&self, table_id: u64) -> Option<&TableMapEvent<'static>> {
        self.esr.get_tme(table_id)
    }
}

impl Iterator for BinlogStream {
    type Item = crate::Result<Event>;

    fn next(&mut self) -> Option<Self::Item> {
        let conn = self.conn.as_mut()?;

        let packet = match conn.read_packet() {
            Ok(packet) => packet,
            Err(err) => {
                self.conn = None;
                return Some(Err(err));
            }
        };

        let first_byte = packet.get(0).copied();

        if first_byte == Some(255) {
            if let Ok(ErrPacket::Error(err)) = ParseBuf(&*packet).parse(conn.0.capability_flags) {
                self.conn = None;
                return Some(Err(crate::Error::MySqlError(From::from(err))));
            }
        }

        if first_byte == Some(254) && packet.len() < 8 {
            if ParseBuf(&*packet)
                .parse::<OkPacketDeserializer<NetworkStreamTerminator>>(conn.0.capability_flags)
                .is_ok()
            {
                self.conn = None;
                return None;
            }
        }

        if first_byte == Some(0) {
            let event_data = &packet[1..];
            match self.esr.read(event_data) {
                Ok(event) => {
                    return Some(Ok(event));
                }
                Err(err) => return Some(Err(err.into())),
            }
        } else {
            self.conn = None;
            return Some(Err(crate::error::DriverError::UnexpectedPacket.into()));
        }
    }
}
