pub static PAYLOAD_OFFSET: usize = 4;
pub static MAX_PACKET_LEN: usize = 16777219;
pub static MAX_PAYLOAD_LEN: usize = 16777215;

pub static UTF8_GENERAL_CI: u8 = 33u8;

/// Server status flags
bitflags! {
    pub flags StatusFlags: u16 {
        const SERVER_STATUS_IN_TRANS             = 0x0001u16,
        const SERVER_STATUS_AUTOCOMMIT           = 0x0002u16,
        const SERVER_MORE_RESULTS_EXISTS         = 0x0008u16,
        const SERVER_STATUS_NO_GOOD_INDEX_USED   = 0x0010u16,
        const SERVER_STATUS_NO_INDEX_USED        = 0x0020u16,
        const SERVER_STATUS_CURSOR_EXISTS        = 0x0040u16,
        const SERVER_STATUS_LAST_ROW_SENT        = 0x0080u16,
        const SERVER_STATUS_DB_DROPPED           = 0x0100u16,
        const SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200u16,
        const SERVER_STATUS_METADATA_CHANGED     = 0x0400u16,
        const SERVER_QUERY_WAS_SLOW              = 0x0800u16,
        const SERVER_PT_OUT_PARAMS               = 0x1000u16,
    }
}

/// Capability flags (u32)
bitflags! {
    pub flags CapabilityFlags: u32 {
        const CLIENT_LONG_PASSWORD                  = 0x00000001u32,
        const CLIENT_FOUND_ROWS                     = 0x00000002u32,
        const CLIENT_LONG_FLAG                      = 0x00000004u32,
        const CLIENT_CONNECT_WITH_DB                = 0x00000008u32,
        const CLIENT_NO_SCHEMA                      = 0x00000010u32,
        const CLIENT_COMPRESS                       = 0x00000020u32,
        const CLIENT_ODBC                           = 0x00000040u32,
        const CLIENT_LOCAL_FILES                    = 0x00000080u32,
        const CLIENT_IGNORE_SPACE                   = 0x00000100u32,
        const CLIENT_PROTOCOL_41                    = 0x00000200u32,
        const CLIENT_INTERACTIVE                    = 0x00000400u32,
        const CLIENT_SSL                            = 0x00000800u32,
        const CLIENT_IGNORE_SIGPIPE                 = 0x00001000u32,
        const CLIENT_TRANSACTIONS                   = 0x00002000u32,
        const CLIENT_RESERVED                       = 0x00004000u32,
        const CLIENT_SECURE_CONNECTION              = 0x00008000u32,
        const CLIENT_MULTI_STATEMENTS               = 0x00010000u32,
        const CLIENT_MULTI_RESULTS                  = 0x00020000u32,
        const CLIENT_PS_MULTI_RESULTS               = 0x00040000u32,
        const CLIENT_PLUGIN_AUTH                    = 0x00080000u32,
        const CLIENT_CONNECT_ATTRS                  = 0x00100000u32,
        const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000u32,
    }
}

/// Commands (u8)
#[allow(non_camel_case_types)]
#[derive(Clone, Eq, PartialEq, Copy, Debug)]
#[repr(u8)]
pub enum Command {
    COM_SLEEP               = 0x00_u8,
    COM_QUIT                = 0x01_u8,
    COM_INIT_DB             = 0x02_u8,
    COM_QUERY               = 0x03_u8,
    COM_FIELD_LIST          = 0x04_u8,
    COM_CREATE_DB           = 0x05_u8,
    COM_DROP_DB             = 0x06_u8,
    COM_REFRESH             = 0x07_u8,
    COM_SHUTDOWN            = 0x08_u8,
    COM_STATISTICS          = 0x09_u8,
    COM_PROCESS_INFO        = 0x0a_u8,
    COM_CONNECT             = 0x0b_u8,
    COM_PROCESS_KILL        = 0x0c_u8,
    COM_DEBUG               = 0x0d_u8,
    COM_PING                = 0x0e_u8,
    COM_TIME                = 0x0f_u8,
    COM_DELAYED_INSERT      = 0x10_u8,
    COM_CHANGE_USER         = 0x11_u8,
    COM_BINLOG_DUMP         = 0x12_u8,
    COM_TABLE_DUMP          = 0x13_u8,
    COM_CONNECT_OUT         = 0x14_u8,
    COM_REGISTER_SLAVE      = 0x15_u8,
    COM_STMT_PREPARE        = 0x16_u8,
    COM_STMT_EXECUTE        = 0x17_u8,
    COM_STMT_SEND_LONG_DATA = 0x18_u8,
    COM_STMT_CLOSE          = 0x19_u8,
    COM_STMT_RESET          = 0x1a_u8,
    COM_SET_OPTION          = 0x1b_u8,
    COM_STMT_FETCH          = 0x1c_u8,
    COM_DAEMON              = 0x1d_u8,
    COM_BINLOG_DUMP_GTID    = 0x1e_u8,
    COM_RESET_CONNECTION    = 0x1f_u8,
}

/// Text protocol column types (u8)
#[allow(non_camel_case_types)]
#[derive(Clone, Eq, PartialEq, Copy, Debug)]
#[repr(u8)]
pub enum ColumnType {
    MYSQL_TYPE_DECIMAL     = 0x00_u8,
    MYSQL_TYPE_TINY        = 0x01_u8,
    MYSQL_TYPE_SHORT       = 0x02_u8,
    MYSQL_TYPE_LONG        = 0x03_u8,
    MYSQL_TYPE_FLOAT       = 0x04_u8,
    MYSQL_TYPE_DOUBLE      = 0x05_u8,
    MYSQL_TYPE_NULL        = 0x06_u8,
    MYSQL_TYPE_TIMESTAMP   = 0x07_u8,
    MYSQL_TYPE_LONGLONG    = 0x08_u8,
    MYSQL_TYPE_INT24       = 0x09_u8,
    MYSQL_TYPE_DATE        = 0x0a_u8,
    MYSQL_TYPE_TIME        = 0x0b_u8,
    MYSQL_TYPE_DATETIME    = 0x0c_u8,
    MYSQL_TYPE_YEAR        = 0x0d_u8,
    MYSQL_TYPE_VARCHAR     = 0x0f_u8,
    MYSQL_TYPE_BIT         = 0x10_u8,
    MYSQL_TYPE_NEWDECIMAL  = 0xf6_u8,
    MYSQL_TYPE_ENUM        = 0xf7_u8,
    MYSQL_TYPE_SET         = 0xf8_u8,
    MYSQL_TYPE_TINY_BLOB   = 0xf9_u8,
    MYSQL_TYPE_MEDIUM_BLOB = 0xfa_u8,
    MYSQL_TYPE_LONG_BLOB   = 0xfb_u8,
    MYSQL_TYPE_BLOB        = 0xfc_u8,
    MYSQL_TYPE_VAR_STRING  = 0xfd_u8,
    MYSQL_TYPE_STRING      = 0xfe_u8,
    MYSQL_TYPE_GEOMETRY    = 0xff_u8,
}

impl From<u8> for ColumnType {
    fn from(x: u8) -> ColumnType {
        match x {
            0x00_u8 => ColumnType::MYSQL_TYPE_DECIMAL,
            0x01_u8 => ColumnType::MYSQL_TYPE_TINY,
            0x02_u8 => ColumnType::MYSQL_TYPE_SHORT,
            0x03_u8 => ColumnType::MYSQL_TYPE_LONG,
            0x04_u8 => ColumnType::MYSQL_TYPE_FLOAT,
            0x05_u8 => ColumnType::MYSQL_TYPE_DOUBLE,
            0x06_u8 => ColumnType::MYSQL_TYPE_NULL,
            0x07_u8 => ColumnType::MYSQL_TYPE_TIMESTAMP,
            0x08_u8 => ColumnType::MYSQL_TYPE_LONGLONG,
            0x09_u8 => ColumnType::MYSQL_TYPE_INT24,
            0x0a_u8 => ColumnType::MYSQL_TYPE_DATE,
            0x0b_u8 => ColumnType::MYSQL_TYPE_TIME,
            0x0c_u8 => ColumnType::MYSQL_TYPE_DATETIME,
            0x0d_u8 => ColumnType::MYSQL_TYPE_YEAR,
            0x0f_u8 => ColumnType::MYSQL_TYPE_VARCHAR,
            0x10_u8 => ColumnType::MYSQL_TYPE_BIT,
            0xf6_u8 => ColumnType::MYSQL_TYPE_NEWDECIMAL,
            0xf7_u8 => ColumnType::MYSQL_TYPE_ENUM,
            0xf8_u8 => ColumnType::MYSQL_TYPE_SET,
            0xf9_u8 => ColumnType::MYSQL_TYPE_TINY_BLOB,
            0xfa_u8 => ColumnType::MYSQL_TYPE_MEDIUM_BLOB,
            0xfb_u8 => ColumnType::MYSQL_TYPE_LONG_BLOB,
            0xfc_u8 => ColumnType::MYSQL_TYPE_BLOB,
            0xfd_u8 => ColumnType::MYSQL_TYPE_VAR_STRING,
            0xfe_u8 => ColumnType::MYSQL_TYPE_STRING,
            0xff_u8 => ColumnType::MYSQL_TYPE_GEOMETRY,
            _ => panic!("Unknown column type"),
        }
    }
}

/// Column flags (u16)
bitflags! {
    pub flags ColumnFlags: u16 {
        const NOT_NULL_FLAG         = 1u16,
        const PRI_KEY_FLAG          = 2u16,
        const UNIQUE_KEY_FLAG       = 4u16,
        const MULTIPLE_KEY_FLAG     = 8u16,
        const BLOB_FLAG             = 16u16,
        const UNSIGNED_FLAG         = 32u16,
        const ZEROFILL_FLAG         = 64u16,
        const BINARY_FLAG           = 128u16,
        const ENUM_FLAG             = 256u16,
        const AUTO_INCREMENT_FLAG   = 512u16,
        const TIMESTAMP_FLAG        = 1024u16,
        const SET_FLAG              = 2048u16,
        const NO_DEFAULT_VALUE_FLAG = 4096u16,
        const ON_UPDATE_NOW_FLAG    = 8192u16,
        const NUM_FLAG              = 32768u16,
        const PART_KEY_FLAG         = 16384u16,
    }
}
