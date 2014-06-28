pub static PAYLOAD_OFFSET: uint = 4;
pub static MAX_PACKET_LEN: uint = 16777219;
pub static MAX_PAYLOAD_LEN: uint = 16777215;

pub static UTF8_GENERAL_CI: u8 = 33u8;

/// Server status flags
#[allow(non_camel_case_types)]
#[deriving(Clone, FromPrimitive)]
pub enum StatusFlag {
    SERVER_STATUS_IN_TRANS             = 0x0001u16,
    SERVER_STATUS_AUTOCOMMIT           = 0x0002u16,
    SERVER_MORE_RESULTS_EXISTS         = 0x0008u16,
    SERVER_STATUS_NO_GOOD_INDEX_USED   = 0x0010u16,
    SERVER_STATUS_NO_INDEX_USED        = 0x0020u16,
    SERVER_STATUS_CURSOR_EXISTS        = 0x0040u16,
    SERVER_STATUS_LAST_ROW_SENT        = 0x0080u16,
    SERVER_STATUS_DB_DROPPED           = 0x0100u16,
    SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200u16,
    SERVER_STATUS_METADATA_CHANGED     = 0x0400u16,
    SERVER_QUERY_WAS_SLOW              = 0x0800u16,
    SERVER_PT_OUT_PARAMS               = 0x1000u16,
}

/// Capability flags (u32)
#[allow(non_camel_case_types)]
#[deriving(Clone, FromPrimitive)]
pub enum CapabilityFlag {
    CLIENT_LONG_PASSWORD                  = 0x00000001u32,
    CLIENT_FOUND_ROWS                     = 0x00000002u32,
    CLIENT_LONG_FLAG                      = 0x00000004u32,
    CLIENT_CONNECT_WITH_DB                = 0x00000008u32,
    CLIENT_NO_SCHEMA                      = 0x00000010u32,
    CLIENT_COMPRESS                       = 0x00000020u32,
    CLIENT_ODBC                           = 0x00000040u32,
    CLIENT_LOCAL_FILES                    = 0x00000080u32,
    CLIENT_IGNORE_SPACE                   = 0x00000100u32,
    CLIENT_PROTOCOL_41                    = 0x00000200u32,
    CLIENT_INTERACTIVE                    = 0x00000400u32,
    CLIENT_SSL                            = 0x00000800u32,
    CLIENT_IGNORE_SIGPIPE                 = 0x00001000u32,
    CLIENT_TRANSACTIONS                   = 0x00002000u32,
    CLIENT_RESERVED                       = 0x00004000u32,
    CLIENT_SECURE_CONNECTION              = 0x00008000u32,
    CLIENT_MULTI_STATEMENTS               = 0x00010000u32,
    CLIENT_MULTI_RESULTS                  = 0x00020000u32,
    CLIENT_PS_MULTI_RESULTS               = 0x00040000u32,
    CLIENT_PLUGIN_AUTH                    = 0x00080000u32,
    CLIENT_CONNECT_ATTRS                  = 0x00100000u32,
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000u32,
}

/// Commands
pub static COM_SLEEP: u8 = 0x00u8;
pub static COM_QUIT: u8 = 0x01_u8;
pub static COM_INIT_DB: u8 = 0x02_u8;
pub static COM_QUERY: u8 = 0x03_u8;
pub static COM_FIELD_LIST: u8 = 0x04_u8;
pub static COM_CREATE_DB: u8 = 0x05_u8;
pub static COM_DROP_DB: u8 = 0x06_u8;
pub static COM_REFRESH: u8 = 0x07_u8;
pub static COM_SHUTDOWN: u8 = 0x08_u8;
pub static COM_STATISTICS: u8 = 0x09_u8;
pub static COM_PROCESS_INFO: u8 = 0x0a_u8;
pub static COM_CONNECT: u8 = 0x0b_u8;
pub static COM_PROCESS_KILL: u8 = 0x0c_u8;
pub static COM_DEBUG: u8 = 0x0d_u8;
pub static COM_PING: u8 = 0x0e_u8;
pub static COM_TIME: u8 = 0x0f_u8;
pub static COM_DELAYED_INSERT: u8 = 0x10_u8;
pub static COM_CHANGE_USER: u8 = 0x11_u8;
pub static COM_BINLOG_DUMP: u8 = 0x12_u8;
pub static COM_TABLE_DUMP: u8 = 0x13_u8;
pub static COM_CONNECT_OUT: u8 = 0x14_u8;
pub static COM_REGISTER_SLAVE: u8 = 0x15_u8;
pub static COM_STMT_PREPARE: u8 = 0x16_u8;
pub static COM_STMT_EXECUTE: u8 = 0x17_u8;
pub static COM_STMT_SEND_LONG_DATA: u8 = 0x18_u8;
pub static COM_STMT_CLOSE: u8 = 0x19_u8;
pub static COM_STMT_RESET: u8 = 0x1a_u8;
pub static COM_SET_OPTION: u8 = 0x1b_u8;
pub static COM_STMT_FETCH: u8 = 0x1c_u8;
pub static COM_DAEMON: u8 = 0x1d_u8;
pub static COM_BINLOG_DUMP_GTID: u8 = 0x1e_u8;
pub static COM_RESET_CONNECTION: u8 = 0x1f_u8;

/// Text protocol column types
#[allow(non_camel_case_types)]
#[deriving(Clone, FromPrimitive)]
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

/// Column flags
pub static NOT_NULL_FLAG: u16 = 1u16;
pub static PRI_KEY_FLAG: u16 = 2u16;
pub static UNIQUE_KEY_FLAG: u16 = 4u16;
pub static MULTIPLE_KEY_FLAG: u16 = 8u16;
pub static BLOB_FLAG: u16 = 16u16;
pub static UNSIGNED_FLAG: u16 = 32u16;
pub static ZEROFILL_FLAG: u16 = 64u16;
pub static BINARY_FLAG: u16 = 128u16;
pub static ENUM_FLAG: u16 = 256u16;
pub static AUTO_INCREMENT_FLAG: u16 = 512u16;
pub static TIMESTAMP_FLAG: u16 = 1024u16;
pub static SET_FLAG: u16 = 2048u16;
pub static NO_DEFAULT_VALUE_FLAG: u16 = 4096u16;
pub static ON_UPDATE_NOW_FLAG: u16 = 8192u16;
pub static NUM_FLAG: u16 = 32768u16;
pub static PART_KEY_FLAG: u16 = 16384u16;
pub static GROUP_FLAG: u16 = 32768u16;
// pub static UNIQUE_FLAG: u16 = 65536u16;
