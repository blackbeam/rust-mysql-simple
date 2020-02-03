use std::io::Cursor;
use byteorder::{LittleEndian as LE, WriteBytesExt, ReadBytesExt};

use crate::conn::Conn;

pub struct SlaveConnectionOptions {
    server_id: u32,
    slave_hostname: String,
    username: String,
    password: String,
    port: u16,
    replication_rank: u32,
    master_id: u32,
}

impl SlaveConnectionOptions {
    fn new_with_default() -> Self {
        SlaveConnectionOptions {
            server_id: 0,
            slave_hostname: String::new(),
            username: String::new(),
            password: String::new(),
            port: 0,
            replication_rank: 0,
            master_id: 0,
        }
    }
}

pub struct SlaveConnectionOptionsBuilder {
    slave_connection_options: SlaveConnectionOptions,
}

impl SlaveConnectionOptionsBuilder {
    pub fn new() -> Self {
        SlaveConnectionOptionsBuilder {
            slave_connection_options: SlaveConnectionOptions::new_with_default(),
        }
    }

    pub fn with_server_id(mut self, server_id: u32) -> Self {
        self.slave_connection_options.server_id = server_id;
        self
    }

    pub fn with_slave_hostname<T: Into<String>>(mut self, slave_hostname: T) -> Self {
        self.slave_connection_options.slave_hostname = slave_hostname.into();
        self
    }

    pub fn with_username<T: Into<String>>(mut self, username: T) -> Self {
        self.slave_connection_options.username = username.into();
        self
    }

    pub fn with_password<T: Into<String>>(mut self, password: T) -> Self {
        self.slave_connection_options.password = password.into();
        self
    }
    pub fn with_port(mut self, port: u16) -> Self {
        self.slave_connection_options.port = port;
        self
    }
    pub fn with_replication_rank(mut self, replication_rank: u32) -> Self {
        self.slave_connection_options.replication_rank = replication_rank;
        self
    }
    pub fn with_master_id(mut self, master_id: u32) -> Self {
        self.slave_connection_options.master_id = master_id;
        self
    }

    pub fn build(self) -> SlaveConnectionOptions {
        self.slave_connection_options
    }
}

pub struct RegisterSlavePacket {
    data: Vec<u8>,
}

impl RegisterSlavePacket {
    pub fn new(connection_options: SlaveConnectionOptions) -> Self {
        let mut buffer = Vec::new();

        buffer.write_u32::<LE>(connection_options.server_id).unwrap();
        buffer.write_u8(connection_options.slave_hostname.len() as u8).unwrap();
        connection_options
            .slave_hostname
            .as_bytes()
            .iter()
            .for_each(|data| buffer.write_u8(*data).unwrap());

        buffer.write_u8(connection_options.username.len() as u8).unwrap();
        connection_options
            .username
            .as_bytes()
            .iter()
            .for_each(|data| buffer.write_u8(*data).unwrap());
        
        buffer.write_u8(connection_options.password.len() as u8).unwrap();
        connection_options
            .password
            .as_bytes()
            .iter()
            .for_each(|data| buffer.write_u8(*data).unwrap());

        buffer.write_u16::<LE>(connection_options.port).unwrap();
        buffer.write_u32::<LE>(connection_options.replication_rank).unwrap();
        buffer.write_u32::<LE>(connection_options.master_id).unwrap();

        RegisterSlavePacket {
            data: buffer,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data[..]
    }
}

pub struct BinlogDumpPacket {
    data: Vec<u8>,
}

impl BinlogDumpPacket {
    pub fn new(position: u32, server_id: u32) -> Self {
        let mut buffer = Vec::new();
        buffer.write_u32::<LE>(position).unwrap();
        buffer.write_u16::<LE>(0).unwrap();
        buffer.write_u32::<LE>(server_id).unwrap();
        BinlogDumpPacket {data: buffer}
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data[..]
    }
}


#[derive(Debug)]
pub struct EventHeader {
    timestamp: u32,
    event_type: u8,
    server_id: u32,
    event_size: u32,
    log_position: u32,
    flags: u16
}

impl EventHeader {
    pub fn from_cursor(cursor: &mut Cursor<Vec<u8>>) -> Self {
        EventHeader {
            timestamp: cursor.read_u32::<LE>().unwrap(),
            event_type: cursor.read_u8().unwrap(),
            server_id: cursor.read_u32::<LE>().unwrap(),
            event_size: cursor.read_u32::<LE>().unwrap(),
            log_position: cursor.read_u32::<LE>().unwrap(),
            flags: cursor.read_u16::<LE>().unwrap(),
        }
    }
}

#[derive(Debug)]
pub struct UnknownEvent {
    pub data: Vec<u8>,
}

impl UnknownEvent {
    fn from_cursor(event_size: u32, cursor: &mut Cursor<Vec<u8>>) -> Self {
        UnknownEvent {
            data : (0 .. event_size - 19)
                .map(|_| cursor.read_u8().unwrap_or(0x0))
                .collect()
        }
    }
}

#[derive(Debug)]
pub enum IntvarEventType {
    InvalidIntEvent = 0,
    LastInsertIdEvent,
    InsertIdEvent,
}

#[derive(Debug)]
pub struct IntvarEvent {
    pub intvar_type: IntvarEventType,
    pub value: u64,
}

impl IntvarEvent {
    fn from_cursor(cursor: &mut Cursor<Vec<u8>>) -> Self {
        let type_value = cursor.read_u8().unwrap();
        let intvar_type = match type_value {
            0x00 => IntvarEventType::InvalidIntEvent,
            0x01 => IntvarEventType::LastInsertIdEvent,
            0x02 => IntvarEventType::InsertIdEvent,
            e => panic!("Invalid Intvar Event Type found {}", e),
        };
        IntvarEvent {
            intvar_type,
            value: cursor.read_u64::<LE>().unwrap()
        }
    }
}

#[derive(Debug)]
pub struct XidEvent {
    pub value: u64,
}

impl XidEvent {
    fn from_cursor(cursor: &mut Cursor<Vec<u8>>) -> Self {
        XidEvent {
            value: cursor.read_u64::<LE>().unwrap()
        }
    }
}

#[derive(Debug)]
pub struct QueryEvent {
    pub slave_proxy_id: u32,
    pub execution_time: u32,
    pub error_code: u16,
    pub status_vars: Vec<u8>,
    pub schema: String,
    pub query: String,
}

impl QueryEvent {
    fn from_cursor(cursor: &mut Cursor<Vec<u8>>) -> Self {
        let slave_proxy_id = cursor.read_u32::<LE>().unwrap();
        let execution_time = cursor.read_u32::<LE>().unwrap();
        let schema_length = cursor.read_u8().unwrap();
        let error_code = cursor.read_u16::<LE>().unwrap();
        let status_var_length= cursor.read_u16::<LE>().unwrap();
        let status_vars: Vec<u8> = (0 .. status_var_length)
            .map(|_| cursor.read_u8().unwrap())
            .collect();
        let raw_schema = (0 .. schema_length)
            .map(|_| cursor.read_u8().unwrap())
            .collect();
        let schema = String::from_utf8(raw_schema).expect("Invalid CharSet");
        
        let _ = cursor.read_u8();
        let mut raw_query = Vec::new();
        loop {
            let new_byte = cursor.read_u8().unwrap_or(0x0);
            if new_byte == 0x0 {
                break;
            }
            raw_query.push(new_byte);
        }

        QueryEvent {
            slave_proxy_id,
            execution_time,
            error_code,
            status_vars,
            schema,
            query: String::from_utf8_lossy(&raw_query).into_owned(),
        }
    }
}

#[derive(Debug)]
pub enum BinLogEventType {
    UnknownEvent(UnknownEvent),
    StartEventV3(UnknownEvent),
    QueryEvent(QueryEvent),
    StopEvent(UnknownEvent),
    RotateEvent(UnknownEvent),
    IntvarEvent(IntvarEvent),
    LoadEvent(UnknownEvent),
    SlaveEvent(UnknownEvent),
    CreateFileEvent(UnknownEvent),
    AppendBlockEvent(UnknownEvent),
    ExecLoadEvent(UnknownEvent),
    DeleteFileEvent(UnknownEvent),
    NewLoadEvent(UnknownEvent),
    RandEvent(UnknownEvent),
    UserVarEvent(UnknownEvent),
    FormatDescriptionEvent(UnknownEvent),
    XidEvent(XidEvent),
    BeginLoadQueryEvent(UnknownEvent),
    ExecuteLoadQueryEvent(UnknownEvent),
    TableMapEvent(UnknownEvent),
    WriteRowsEventV0(UnknownEvent),
    UpdateRowsEventV0(UnknownEvent),
    DeleteRowsEventV0(UnknownEvent),
    WriteRowsEventV1(UnknownEvent),
    UpdateRowsEventV1(UnknownEvent),
    DeleteRowsEventV1(UnknownEvent),
    IncidentEvent(UnknownEvent),
    HeartBeatEvent(UnknownEvent),
    IgnorableEvent(UnknownEvent),
    RowsQueryEvent(UnknownEvent),
    WriteRowsEventV2(UnknownEvent),
    UpdateRowsEventV2(UnknownEvent),
    DeleteRowsEventV2(UnknownEvent),
    GtidEvent(UnknownEvent),
    AnonymouesGtidEvent(UnknownEvent),
    PreviousGtidEvent(UnknownEvent),
}

#[derive(Debug)]
pub struct BinLogEvent {
    pub header: EventHeader,
    pub event_type: BinLogEventType,
}

impl BinLogEvent {
    fn new_from_cursor(mut cursor: Cursor<Vec<u8>>) -> Self {
        let header = EventHeader::from_cursor(&mut cursor);
        let event_type = match header.event_type {
            0x05 => BinLogEventType::IntvarEvent(IntvarEvent::from_cursor(&mut cursor)),
            0x02 => BinLogEventType::QueryEvent(QueryEvent::from_cursor(&mut cursor)),
            0x10 => BinLogEventType::XidEvent(XidEvent::from_cursor(&mut cursor)),
            _ => BinLogEventType::UnknownEvent(UnknownEvent::from_cursor(header.event_size, &mut cursor))
        };
        BinLogEvent {
            header,
            event_type,
        }
    }
}

pub struct BinLogEventIterator {
    conn: Conn,
}

impl BinLogEventIterator {
    pub fn new (conn: Conn) -> Self {
        BinLogEventIterator {conn}
    }
}

impl Iterator for BinLogEventIterator {
    type Item = BinLogEvent;

    fn next(&mut self) -> Option<Self::Item> {
        match self.conn.read_packet() {
            Ok(data) => {
                let mut cursor = Cursor::new(data);
                cursor.read_u8().unwrap();
                Some(BinLogEvent::new_from_cursor(cursor))
            },
            Err(_err) => None,
        }
    }
}