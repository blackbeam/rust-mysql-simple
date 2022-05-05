// Copyright (c) 2020 rust-mysql-simple contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use mysql_common::{
    named_params::MixedParamsError, packets, params::MissingNamedParameterError,
    proto::codec::error::PacketCodecError, row::convert::FromRowError,
    value::convert::FromValueError,
};
use url::ParseError;

use std::{error, fmt, io, result, sync};

use crate::{Row, Value};

pub mod tls;

impl<'a> From<packets::ServerError<'a>> for MySqlError {
    fn from(x: packets::ServerError<'a>) -> MySqlError {
        MySqlError {
            state: x.sql_state_str().into_owned(),
            code: x.error_code(),
            message: x.message_str().into_owned(),
        }
    }
}

#[derive(Eq, PartialEq, Clone)]
pub struct MySqlError {
    pub state: String,
    pub message: String,
    pub code: u16,
}

impl fmt::Display for MySqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ERROR {} ({}): {}", self.code, self.state, self.message)
    }
}

impl fmt::Debug for MySqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl error::Error for MySqlError {
    fn description(&self) -> &str {
        "Error returned by a server"
    }
}

pub enum Error {
    IoError(io::Error),
    CodecError(mysql_common::proto::codec::error::PacketCodecError),
    MySqlError(MySqlError),
    DriverError(DriverError),
    UrlError(UrlError),
    #[cfg(any(feature = "native-tls", feature = "rustls"))]
    TlsError(tls::TlsError),
    FromValueError(Value),
    FromRowError(Row),
}

impl Error {
    #[doc(hidden)]
    pub fn is_connectivity_error(&self) -> bool {
        match self {
            #[cfg(any(feature = "native-tls", feature = "rustls"))]
            Error::TlsError(_) => true,
            Error::IoError(_) | Error::DriverError(_) | Error::CodecError(_) => true,
            Error::MySqlError(_)
            | Error::UrlError(_)
            | Error::FromValueError(_)
            | Error::FromRowError(_) => false,
        }
    }

    #[doc(hidden)]
    pub fn server_disconnected() -> Self {
        Error::IoError(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "server disconnected",
        ))
    }
}

impl error::Error for Error {
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            Error::IoError(ref err) => Some(err),
            Error::DriverError(ref err) => Some(err),
            Error::MySqlError(ref err) => Some(err),
            Error::UrlError(ref err) => Some(err),
            #[cfg(any(feature = "native-tls", feature = "rustls"))]
            Error::TlsError(ref err) => Some(err),
            _ => None,
        }
    }
}

impl From<FromValueError> for Error {
    fn from(FromValueError(value): FromValueError) -> Error {
        Error::FromValueError(value)
    }
}

impl From<FromRowError> for Error {
    fn from(FromRowError(row): FromRowError) -> Error {
        Error::FromRowError(row)
    }
}

impl From<MissingNamedParameterError> for Error {
    fn from(MissingNamedParameterError(name): MissingNamedParameterError) -> Error {
        Error::DriverError(DriverError::MissingNamedParameter(name))
    }
}

impl From<MixedParamsError> for Error {
    fn from(_: MixedParamsError) -> Error {
        Error::DriverError(DriverError::MixedParams)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IoError(err)
    }
}

impl From<DriverError> for Error {
    fn from(err: DriverError) -> Error {
        Error::DriverError(err)
    }
}

impl From<MySqlError> for Error {
    fn from(x: MySqlError) -> Error {
        Error::MySqlError(x)
    }
}

impl From<PacketCodecError> for Error {
    fn from(err: PacketCodecError) -> Self {
        Error::CodecError(err)
    }
}

impl From<std::convert::Infallible> for Error {
    fn from(err: std::convert::Infallible) -> Self {
        match err {}
    }
}

impl From<UrlError> for Error {
    fn from(err: UrlError) -> Error {
        Error::UrlError(err)
    }
}

impl<T> From<sync::PoisonError<T>> for Error {
    fn from(_: sync::PoisonError<T>) -> Error {
        Error::DriverError(DriverError::PoisonedPoolMutex)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Error::IoError(ref err) => write!(f, "IoError {{ {} }}", err),
            Error::CodecError(ref err) => write!(f, "CodecError {{ {} }}", err),
            Error::MySqlError(ref err) => write!(f, "MySqlError {{ {} }}", err),
            Error::DriverError(ref err) => write!(f, "DriverError {{ {} }}", err),
            Error::UrlError(ref err) => write!(f, "UrlError {{ {} }}", err),
            #[cfg(any(feature = "native-tls", feature = "rustls"))]
            Error::TlsError(ref err) => write!(f, "TlsError {{ {} }}", err),
            Error::FromRowError(_) => "from row conversion error".fmt(f),
            Error::FromValueError(_) => "from value conversion error".fmt(f),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Eq, PartialEq, Clone)]
pub enum DriverError {
    ConnectTimeout,
    // (address, description)
    CouldNotConnect(Option<(String, String, io::ErrorKind)>),
    UnsupportedProtocol(u8),
    PacketOutOfSync,
    PacketTooLarge,
    Protocol41NotSet,
    UnexpectedPacket,
    MismatchedStmtParams(u16, usize),
    InvalidPoolConstraints,
    SetupError,
    TlsNotSupported,
    CouldNotParseVersion,
    ReadOnlyTransNotSupported,
    PoisonedPoolMutex,
    Timeout,
    MissingNamedParameter(String),
    NamedParamsForPositionalQuery,
    MixedParams,
    UnknownAuthPlugin(String),
    OldMysqlPasswordDisabled,
}

impl error::Error for DriverError {
    fn description(&self) -> &str {
        "MySql driver error"
    }
}

impl fmt::Display for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            DriverError::ConnectTimeout => write!(f, "Could not connect: connection timeout"),
            DriverError::CouldNotConnect(None) => {
                write!(f, "Could not connect: address not specified")
            }
            DriverError::CouldNotConnect(Some((ref addr, ref desc, _))) => {
                write!(f, "Could not connect to address `{}': {}", addr, desc)
            }
            DriverError::UnsupportedProtocol(proto_version) => {
                write!(f, "Unsupported protocol version {}", proto_version)
            }
            DriverError::PacketOutOfSync => write!(f, "Packet out of sync"),
            DriverError::PacketTooLarge => write!(f, "Packet too large"),
            DriverError::Protocol41NotSet => write!(f, "Server must set CLIENT_PROTOCOL_41 flag"),
            DriverError::UnexpectedPacket => write!(f, "Unexpected packet"),
            DriverError::MismatchedStmtParams(exp, prov) => write!(
                f,
                "Statement takes {} parameters but {} was supplied",
                exp, prov
            ),
            DriverError::InvalidPoolConstraints => write!(f, "Invalid pool constraints"),
            DriverError::SetupError => write!(f, "Could not setup connection"),
            DriverError::TlsNotSupported => write!(
                f,
                "Client requires secure connection but server \
                 does not have this capability"
            ),
            DriverError::CouldNotParseVersion => write!(f, "Could not parse MySQL version"),
            DriverError::ReadOnlyTransNotSupported => write!(
                f,
                "Read-only transactions does not supported in your MySQL version"
            ),
            DriverError::PoisonedPoolMutex => write!(f, "Poisoned pool mutex"),
            DriverError::Timeout => write!(f, "Operation timed out"),
            DriverError::MissingNamedParameter(ref name) => {
                write!(f, "Missing named parameter `{}' for statement", name)
            }
            DriverError::NamedParamsForPositionalQuery => {
                write!(f, "Can not pass named parameters to positional query")
            }
            DriverError::MixedParams => write!(
                f,
                "Can not mix named and positional parameters in one statement"
            ),
            DriverError::UnknownAuthPlugin(ref name) => {
                write!(f, "Unknown authentication protocol: `{}`", name)
            }
            DriverError::OldMysqlPasswordDisabled => {
                write!(
                    f,
                    "`old_mysql_password` plugin is insecure and disabled by default",
                )
            }
        }
    }
}

impl fmt::Debug for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Eq, PartialEq, Clone)]
pub enum UrlError {
    ParseError(ParseError),
    UnsupportedScheme(String),
    /// (feature_name, parameter_name)
    FeatureRequired(String, String),
    /// (feature_name, value)
    InvalidValue(String, String),
    UnknownParameter(String),
    BadUrl,
}

impl error::Error for UrlError {
    fn description(&self) -> &str {
        "Database connection URL error"
    }
}

impl fmt::Display for UrlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            UrlError::ParseError(ref err) => write!(f, "URL ParseError {{ {} }}", err),
            UrlError::UnsupportedScheme(ref s) => write!(f, "URL scheme `{}' is not supported", s),
            UrlError::FeatureRequired(ref feature, ref parameter) => write!(
                f,
                "Url parameter `{}' requires {} feature",
                parameter, feature
            ),
            UrlError::InvalidValue(ref parameter, ref value) => write!(
                f,
                "Invalid value `{}' for URL parameter `{}'",
                value, parameter
            ),
            UrlError::UnknownParameter(ref parameter) => {
                write!(f, "Unknown URL parameter `{}'", parameter)
            }
            UrlError::BadUrl => write!(f, "Invalid or incomplete connection URL"),
        }
    }
}

impl fmt::Debug for UrlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl From<ParseError> for UrlError {
    fn from(x: ParseError) -> UrlError {
        UrlError::ParseError(x)
    }
}

pub type Result<T> = result::Result<T, Error>;

/// Server error codes (u16)
#[allow(non_camel_case_types)]
#[derive(Clone, Eq, PartialEq, Debug, Copy)]
#[repr(u16)]
pub enum ServerError {
    ER_HASHCHK = 1000u16,
    ER_NISAMCHK = 1001u16,
    ER_NO = 1002u16,
    ER_YES = 1003u16,
    ER_CANT_CREATE_FILE = 1004u16,
    ER_CANT_CREATE_TABLE = 1005u16,
    ER_CANT_CREATE_DB = 1006u16,
    ER_DB_CREATE_EXISTS = 1007u16,
    ER_DB_DROP_EXISTS = 1008u16,
    ER_DB_DROP_DELETE = 1009u16,
    ER_DB_DROP_RMDIR = 1010u16,
    ER_CANT_DELETE_FILE = 1011u16,
    ER_CANT_FIND_SYSTEM_REC = 1012u16,
    ER_CANT_GET_STAT = 1013u16,
    ER_CANT_GET_WD = 1014u16,
    ER_CANT_LOCK = 1015u16,
    ER_CANT_OPEN_FILE = 1016u16,
    ER_FILE_NOT_FOUND = 1017u16,
    ER_CANT_READ_DIR = 1018u16,
    ER_CANT_SET_WD = 1019u16,
    ER_CHECKREAD = 1020u16,
    ER_DISK_FULL = 1021u16,
    ER_DUP_KEY = 1022u16,
    ER_ERROR_ON_CLOSE = 1023u16,
    ER_ERROR_ON_READ = 1024u16,
    ER_ERROR_ON_RENAME = 1025u16,
    ER_ERROR_ON_WRITE = 1026u16,
    ER_FILE_USED = 1027u16,
    ER_FILSORT_ABORT = 1028u16,
    ER_FORM_NOT_FOUND = 1029u16,
    ER_GET_ERRNO = 1030u16,
    ER_ILLEGAL_HA = 1031u16,
    ER_KEY_NOT_FOUND = 1032u16,
    ER_NOT_FORM_FILE = 1033u16,
    ER_NOT_KEYFILE = 1034u16,
    ER_OLD_KEYFILE = 1035u16,
    ER_OPEN_AS_READONLY = 1036u16,
    ER_OUTOFMEMORY = 1037u16,
    ER_OUT_OF_SORTMEMORY = 1038u16,
    ER_UNEXPECTED_EOF = 1039u16,
    ER_CON_COUNT_ERROR = 1040u16,
    ER_OUT_OF_RESOURCES = 1041u16,
    ER_BAD_HOST_ERROR = 1042u16,
    ER_HANDSHAKE_ERROR = 1043u16,
    ER_DBACCESS_DENIED_ERROR = 1044u16,
    ER_ACCESS_DENIED_ERROR = 1045u16,
    ER_NO_DB_ERROR = 1046u16,
    ER_UNKNOWN_COM_ERROR = 1047u16,
    ER_BAD_NULL_ERROR = 1048u16,
    ER_BAD_DB_ERROR = 1049u16,
    ER_TABLE_EXISTS_ERROR = 1050u16,
    ER_BAD_TABLE_ERROR = 1051u16,
    ER_NON_UNIQ_ERROR = 1052u16,
    ER_SERVER_SHUTDOWN = 1053u16,
    ER_BAD_FIELD_ERROR = 1054u16,
    ER_WRONG_FIELD_WITH_GROUP = 1055u16,
    ER_WRONG_GROUP_FIELD = 1056u16,
    ER_WRONG_SUM_SELECT = 1057u16,
    ER_WRONG_VALUE_COUNT = 1058u16,
    ER_TOO_LONG_IDENT = 1059u16,
    ER_DUP_FIELDNAME = 1060u16,
    ER_DUP_KEYNAME = 1061u16,
    ER_DUP_ENTRY = 1062u16,
    ER_WRONG_FIELD_SPEC = 1063u16,
    ER_PARSE_ERROR = 1064u16,
    ER_EMPTY_QUERY = 1065u16,
    ER_NONUNIQ_TABLE = 1066u16,
    ER_INVALID_DEFAULT = 1067u16,
    ER_MULTIPLE_PRI_KEY = 1068u16,
    ER_TOO_MANY_KEYS = 1069u16,
    ER_TOO_MANY_KEY_PARTS = 1070u16,
    ER_TOO_LONG_KEY = 1071u16,
    ER_KEY_COLUMN_DOES_NOT_EXITS = 1072u16,
    ER_BLOB_USED_AS_KEY = 1073u16,
    ER_TOO_BIG_FIELDLENGTH = 1074u16,
    ER_WRONG_AUTO_KEY = 1075u16,
    ER_READY = 1076u16,
    ER_NORMAL_SHUTDOWN = 1077u16,
    ER_GOT_SIGNAL = 1078u16,
    ER_SHUTDOWN_COMPLETE = 1079u16,
    ER_FORCING_CLOSE = 1080u16,
    ER_IPSOCK_ERROR = 1081u16,
    ER_NO_SUCH_INDEX = 1082u16,
    ER_WRONG_FIELD_TERMINATORS = 1083u16,
    ER_BLOBS_AND_NO_TERMINATED = 1084u16,
    ER_TEXTFILE_NOT_READABLE = 1085u16,
    ER_FILE_EXISTS_ERROR = 1086u16,
    ER_LOAD_INFO = 1087u16,
    ER_ALTER_INFO = 1088u16,
    ER_WRONG_SUB_KEY = 1089u16,
    ER_CANT_REMOVE_ALL_FIELDS = 1090u16,
    ER_CANT_DROP_FIELD_OR_KEY = 1091u16,
    ER_INSERT_INFO = 1092u16,
    ER_UPDATE_TABLE_USED = 1093u16,
    ER_NO_SUCH_THREAD = 1094u16,
    ER_KILL_DENIED_ERROR = 1095u16,
    ER_NO_TABLES_USED = 1096u16,
    ER_TOO_BIG_SET = 1097u16,
    ER_NO_UNIQUE_LOGFILE = 1098u16,
    ER_TABLE_NOT_LOCKED_FOR_WRITE = 1099u16,
    ER_TABLE_NOT_LOCKED = 1100u16,
    ER_BLOB_CANT_HAVE_DEFAULT = 1101u16,
    ER_WRONG_DB_NAME = 1102u16,
    ER_WRONG_TABLE_NAME = 1103u16,
    ER_TOO_BIG_SELECT = 1104u16,
    ER_UNKNOWN_ERROR = 1105u16,
    ER_UNKNOWN_PROCEDURE = 1106u16,
    ER_WRONG_PARAMCOUNT_TO_PROCEDURE = 1107u16,
    ER_WRONG_PARAMETERS_TO_PROCEDURE = 1108u16,
    ER_UNKNOWN_TABLE = 1109u16,
    ER_FIELD_SPECIFIED_TWICE = 1110u16,
    ER_INVALID_GROUP_FUNC_USE = 1111u16,
    ER_UNSUPPORTED_EXTENSION = 1112u16,
    ER_TABLE_MUST_HAVE_COLUMNS = 1113u16,
    ER_RECORD_FILE_FULL = 1114u16,
    ER_UNKNOWN_CHARACTER_SET = 1115u16,
    ER_TOO_MANY_TABLES = 1116u16,
    ER_TOO_MANY_FIELDS = 1117u16,
    ER_TOO_BIG_ROWSIZE = 1118u16,
    ER_STACK_OVERRUN = 1119u16,
    ER_WRONG_OUTER_JOIN = 1120u16,
    ER_NULL_COLUMN_IN_INDEX = 1121u16,
    ER_CANT_FIND_UDF = 1122u16,
    ER_CANT_INITIALIZE_UDF = 1123u16,
    ER_UDF_NO_PATHS = 1124u16,
    ER_UDF_EXISTS = 1125u16,
    ER_CANT_OPEN_LIBRARY = 1126u16,
    ER_CANT_FIND_DL_ENTRY = 1127u16,
    ER_FUNCTION_NOT_DEFINED = 1128u16,
    ER_HOST_IS_BLOCKED = 1129u16,
    ER_HOST_NOT_PRIVILEGED = 1130u16,
    ER_PASSWORD_ANONYMOUS_USER = 1131u16,
    ER_PASSWORD_NOT_ALLOWED = 1132u16,
    ER_PASSWORD_NO_MATCH = 1133u16,
    ER_UPDATE_INFO = 1134u16,
    ER_CANT_CREATE_THREAD = 1135u16,
    ER_WRONG_VALUE_COUNT_ON_ROW = 1136u16,
    ER_CANT_REOPEN_TABLE = 1137u16,
    ER_INVALID_USE_OF_NULL = 1138u16,
    ER_REGEXP_ERROR = 1139u16,
    ER_MIX_OF_GROUP_FUNC_AND_FIELDS = 1140u16,
    ER_NONEXISTING_GRANT = 1141u16,
    ER_TABLEACCESS_DENIED_ERROR = 1142u16,
    ER_COLUMNACCESS_DENIED_ERROR = 1143u16,
    ER_ILLEGAL_GRANT_FOR_TABLE = 1144u16,
    ER_GRANT_WRONG_HOST_OR_USER = 1145u16,
    ER_NO_SUCH_TABLE = 1146u16,
    ER_NONEXISTING_TABLE_GRANT = 1147u16,
    ER_NOT_ALLOWED_COMMAND = 1148u16,
    ER_SYNTAX_ERROR = 1149u16,
    ER_DELAYED_CANT_CHANGE_LOCK = 1150u16,
    ER_TOO_MANY_DELAYED_THREADS = 1151u16,
    ER_ABORTING_CONNECTION = 1152u16,
    ER_NET_PACKET_TOO_LARGE = 1153u16,
    ER_NET_READ_ERROR_FROM_PIPE = 1154u16,
    ER_NET_FCNTL_ERROR = 1155u16,
    ER_NET_PACKETS_OUT_OF_ORDER = 1156u16,
    ER_NET_UNCOMPRESS_ERROR = 1157u16,
    ER_NET_READ_ERROR = 1158u16,
    ER_NET_READ_INTERRUPTED = 1159u16,
    ER_NET_ERROR_ON_WRITE = 1160u16,
    ER_NET_WRITE_INTERRUPTED = 1161u16,
    ER_TOO_LONG_STRING = 1162u16,
    ER_TABLE_CANT_HANDLE_BLOB = 1163u16,
    ER_TABLE_CANT_HANDLE_AUTO_INCREMENT = 1164u16,
    ER_DELAYED_INSERT_TABLE_LOCKED = 1165u16,
    ER_WRONG_COLUMN_NAME = 1166u16,
    ER_WRONG_KEY_COLUMN = 1167u16,
    ER_WRONG_MRG_TABLE = 1168u16,
    ER_DUP_UNIQUE = 1169u16,
    ER_BLOB_KEY_WITHOUT_LENGTH = 1170u16,
    ER_PRIMARY_CANT_HAVE_NULL = 1171u16,
    ER_TOO_MANY_ROWS = 1172u16,
    ER_REQUIRES_PRIMARY_KEY = 1173u16,
    ER_NO_RAID_COMPILED = 1174u16,
    ER_UPDATE_WITHOUT_KEY_IN_SAFE_MODE = 1175u16,
    ER_KEY_DOES_NOT_EXITS = 1176u16,
    ER_CHECK_NO_SUCH_TABLE = 1177u16,
    ER_CHECK_NOT_IMPLEMENTED = 1178u16,
    ER_CANT_DO_THIS_DURING_AN_TRANSACTION = 1179u16,
    ER_ERROR_DURING_COMMIT = 1180u16,
    ER_ERROR_DURING_ROLLBACK = 1181u16,
    ER_ERROR_DURING_FLUSH_LOGS = 1182u16,
    ER_ERROR_DURING_CHECKPOINT = 1183u16,
    ER_NEW_ABORTING_CONNECTION = 1184u16,
    ER_DUMP_NOT_IMPLEMENTED = 1185u16,
    ER_FLUSH_MASTER_BINLOG_CLOSED = 1186u16,
    ER_INDEX_REBUILD = 1187u16,
    ER_MASTER = 1188u16,
    ER_MASTER_NET_READ = 1189u16,
    ER_MASTER_NET_WRITE = 1190u16,
    ER_FT_MATCHING_KEY_NOT_FOUND = 1191u16,
    ER_LOCK_OR_ACTIVE_TRANSACTION = 1192u16,
    ER_UNKNOWN_SYSTEM_VARIABLE = 1193u16,
    ER_CRASHED_ON_USAGE = 1194u16,
    ER_CRASHED_ON_REPAIR = 1195u16,
    ER_WARNING_NOT_COMPLETE_ROLLBACK = 1196u16,
    ER_TRANS_CACHE_FULL = 1197u16,
    ER_SLAVE_MUST_STOP = 1198u16,
    ER_SLAVE_NOT_RUNNING = 1199u16,
    ER_BAD_SLAVE = 1200u16,
    ER_MASTER_INFO = 1201u16,
    ER_SLAVE_THREAD = 1202u16,
    ER_TOO_MANY_USER_CONNECTIONS = 1203u16,
    ER_SET_CONSTANTS_ONLY = 1204u16,
    ER_LOCK_WAIT_TIMEOUT = 1205u16,
    ER_LOCK_TABLE_FULL = 1206u16,
    ER_READ_ONLY_TRANSACTION = 1207u16,
    ER_DROP_DB_WITH_READ_LOCK = 1208u16,
    ER_CREATE_DB_WITH_READ_LOCK = 1209u16,
    ER_WRONG_ARGUMENTS = 1210u16,
    ER_NO_PERMISSION_TO_CREATE_USER = 1211u16,
    ER_UNION_TABLES_IN_DIFFERENT_DIR = 1212u16,
    ER_LOCK_DEADLOCK = 1213u16,
    ER_TABLE_CANT_HANDLE_FT = 1214u16,
    ER_CANNOT_ADD_FOREIGN = 1215u16,
    ER_NO_REFERENCED_ROW = 1216u16,
    ER_ROW_IS_REFERENCED = 1217u16,
    ER_CONNECT_TO_MASTER = 1218u16,
    ER_QUERY_ON_MASTER = 1219u16,
    ER_ERROR_WHEN_EXECUTING_COMMAND = 1220u16,
    ER_WRONG_USAGE = 1221u16,
    ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT = 1222u16,
    ER_CANT_UPDATE_WITH_READLOCK = 1223u16,
    ER_MIXING_NOT_ALLOWED = 1224u16,
    ER_DUP_ARGUMENT = 1225u16,
    ER_USER_LIMIT_REACHED = 1226u16,
    ER_SPECIFIC_ACCESS_DENIED_ERROR = 1227u16,
    ER_LOCAL_VARIABLE = 1228u16,
    ER_GLOBAL_VARIABLE = 1229u16,
    ER_NO_DEFAULT = 1230u16,
    ER_WRONG_VALUE_FOR_VAR = 1231u16,
    ER_WRONG_TYPE_FOR_VAR = 1232u16,
    ER_VAR_CANT_BE_READ = 1233u16,
    ER_CANT_USE_OPTION_HERE = 1234u16,
    ER_NOT_SUPPORTED_YET = 1235u16,
    ER_MASTER_FATAL_ERROR_READING_BINLOG = 1236u16,
    ER_SLAVE_IGNORED_TABLE = 1237u16,
    ER_INCORRECT_GLOBAL_LOCAL_VAR = 1238u16,
    ER_WRONG_FK_DEF = 1239u16,
    ER_KEY_REF_DO_NOT_MATCH_TABLE_REF = 1240u16,
    ER_OPERAND_COLUMNS = 1241u16,
    ER_SUBQUERY_NO_1_ROW = 1242u16,
    ER_UNKNOWN_STMT_HANDLER = 1243u16,
    ER_CORRUPT_HELP_DB = 1244u16,
    ER_CYCLIC_REFERENCE = 1245u16,
    ER_AUTO_CONVERT = 1246u16,
    ER_ILLEGAL_REFERENCE = 1247u16,
    ER_DERIVED_MUST_HAVE_ALIAS = 1248u16,
    ER_SELECT_REDUCED = 1249u16,
    ER_TABLENAME_NOT_ALLOWED_HERE = 1250u16,
    ER_NOT_SUPPORTED_AUTH_MODE = 1251u16,
    ER_SPATIAL_CANT_HAVE_NULL = 1252u16,
    ER_COLLATION_CHARSET_MISMATCH = 1253u16,
    ER_SLAVE_WAS_RUNNING = 1254u16,
    ER_SLAVE_WAS_NOT_RUNNING = 1255u16,
    ER_TOO_BIG_FOR_UNCOMPRESS = 1256u16,
    ER_ZLIB_Z_MEM_ERROR = 1257u16,
    ER_ZLIB_Z_BUF_ERROR = 1258u16,
    ER_ZLIB_Z_DATA_ERROR = 1259u16,
    ER_CUT_VALUE_GROUP_CONCAT = 1260u16,
    ER_WARN_TOO_FEW_RECORDS = 1261u16,
    ER_WARN_TOO_MANY_RECORDS = 1262u16,
    ER_WARN_NULL_TO_NOTNULL = 1263u16,
    ER_WARN_DATA_OUT_OF_RANGE = 1264u16,
    WARN_DATA_TRUNCATED = 1265u16,
    ER_WARN_USING_OTHER_HANDLER = 1266u16,
    ER_CANT_AGGREGATE_2COLLATIONS = 1267u16,
    ER_DROP_USER = 1268u16,
    ER_REVOKE_GRANTS = 1269u16,
    ER_CANT_AGGREGATE_3COLLATIONS = 1270u16,
    ER_CANT_AGGREGATE_NCOLLATIONS = 1271u16,
    ER_VARIABLE_IS_NOT_STRUCT = 1272u16,
    ER_UNKNOWN_COLLATION = 1273u16,
    ER_SLAVE_IGNORED_SSL_PARAMS = 1274u16,
    ER_SERVER_IS_IN_SECURE_AUTH_MODE = 1275u16,
    ER_WARN_FIELD_RESOLVED = 1276u16,
    ER_BAD_SLAVE_UNTIL_COND = 1277u16,
    ER_MISSING_SKIP_SLAVE = 1278u16,
    ER_UNTIL_COND_IGNORED = 1279u16,
    ER_WRONG_NAME_FOR_INDEX = 1280u16,
    ER_WRONG_NAME_FOR_CATALOG = 1281u16,
    ER_WARN_QC_RESIZE = 1282u16,
    ER_BAD_FT_COLUMN = 1283u16,
    ER_UNKNOWN_KEY_CACHE = 1284u16,
    ER_WARN_HOSTNAME_WONT_WORK = 1285u16,
    ER_UNKNOWN_STORAGE_ENGINE = 1286u16,
    ER_WARN_DEPRECATED_SYNTAX = 1287u16,
    ER_NON_UPDATABLE_TABLE = 1288u16,
    ER_FEATURE_DISABLED = 1289u16,
    ER_OPTION_PREVENTS_STATEMENT = 1290u16,
    ER_DUPLICATED_VALUE_IN_TYPE = 1291u16,
    ER_TRUNCATED_WRONG_VALUE = 1292u16,
    ER_TOO_MUCH_AUTO_TIMESTAMP_COLS = 1293u16,
    ER_INVALID_ON_UPDATE = 1294u16,
    ER_UNSUPPORTED_PS = 1295u16,
    ER_GET_ERRMSG = 1296u16,
    ER_GET_TEMPORARY_ERRMSG = 1297u16,
    ER_UNKNOWN_TIME_ZONE = 1298u16,
    ER_WARN_INVALID_TIMESTAMP = 1299u16,
    ER_INVALID_CHARACTER_STRING = 1300u16,
    ER_WARN_ALLOWED_PACKET_OVERFLOWED = 1301u16,
    ER_CONFLICTING_DECLARATIONS = 1302u16,
    ER_SP_NO_RECURSIVE_CREATE = 1303u16,
    ER_SP_ALREADY_EXISTS = 1304u16,
    ER_SP_DOES_NOT_EXIST = 1305u16,
    ER_SP_DROP_FAILED = 1306u16,
    ER_SP_STORE_FAILED = 1307u16,
    ER_SP_LILABEL_MISMATCH = 1308u16,
    ER_SP_LABEL_REDEFINE = 1309u16,
    ER_SP_LABEL_MISMATCH = 1310u16,
    ER_SP_UNINIT_VAR = 1311u16,
    ER_SP_BADSELECT = 1312u16,
    ER_SP_BADRETURN = 1313u16,
    ER_SP_BADSTATEMENT = 1314u16,
    ER_UPDATE_LOG_DEPRECATED_IGNORED = 1315u16,
    ER_UPDATE_LOG_DEPRECATED_TRANSLATED = 1316u16,
    ER_QUERY_INTERRUPTED = 1317u16,
    ER_SP_WRONG_NO_OF_ARGS = 1318u16,
    ER_SP_COND_MISMATCH = 1319u16,
    ER_SP_NORETURN = 1320u16,
    ER_SP_NORETURNEND = 1321u16,
    ER_SP_BAD_CURSOR_QUERY = 1322u16,
    ER_SP_BAD_CURSOR_SELECT = 1323u16,
    ER_SP_CURSOR_MISMATCH = 1324u16,
    ER_SP_CURSOR_ALREADY_OPEN = 1325u16,
    ER_SP_CURSOR_NOT_OPEN = 1326u16,
    ER_SP_UNDECLARED_VAR = 1327u16,
    ER_SP_WRONG_NO_OF_FETCH_ARGS = 1328u16,
    ER_SP_FETCH_NO_DATA = 1329u16,
    ER_SP_DUP_PARAM = 1330u16,
    ER_SP_DUP_VAR = 1331u16,
    ER_SP_DUP_COND = 1332u16,
    ER_SP_DUP_CURS = 1333u16,
    ER_SP_CANT_ALTER = 1334u16,
    ER_SP_SUBSELECT_NYI = 1335u16,
    ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG = 1336u16,
    ER_SP_VARCOND_AFTER_CURSHNDLR = 1337u16,
    ER_SP_CURSOR_AFTER_HANDLER = 1338u16,
    ER_SP_CASE_NOT_FOUND = 1339u16,
    ER_FPARSER_TOO_BIG_FILE = 1340u16,
    ER_FPARSER_BAD_HEADER = 1341u16,
    ER_FPARSER_EOF_IN_COMMENT = 1342u16,
    ER_FPARSER_ERROR_IN_PARAMETER = 1343u16,
    ER_FPARSER_EOF_IN_UNKNOWN_PARAMETER = 1344u16,
    ER_VIEW_NO_EXPLAIN = 1345u16,
    ER_FRM_UNKNOWN_TYPE = 1346u16,
    ER_WRONG_OBJECT = 1347u16,
    ER_NONUPDATEABLE_COLUMN = 1348u16,
    ER_VIEW_SELECT_DERIVED = 1349u16,
    ER_VIEW_SELECT_CLAUSE = 1350u16,
    ER_VIEW_SELECT_VARIABLE = 1351u16,
    ER_VIEW_SELECT_TMPTABLE = 1352u16,
    ER_VIEW_WRONG_LIST = 1353u16,
    ER_WARN_VIEW_MERGE = 1354u16,
    ER_WARN_VIEW_WITHOUT_KEY = 1355u16,
    ER_VIEW_INVALID = 1356u16,
    ER_SP_NO_DROP_SP = 1357u16,
    ER_SP_GOTO_IN_HNDLR = 1358u16,
    ER_TRG_ALREADY_EXISTS = 1359u16,
    ER_TRG_DOES_NOT_EXIST = 1360u16,
    ER_TRG_ON_VIEW_OR_TEMP_TABLE = 1361u16,
    ER_TRG_CANT_CHANGE_ROW = 1362u16,
    ER_TRG_NO_SUCH_ROW_IN_TRG = 1363u16,
    ER_NO_DEFAULT_FOR_FIELD = 1364u16,
    ER_DIVISION_BY_ZERO = 1365u16,
    ER_TRUNCATED_WRONG_VALUE_FOR_FIELD = 1366u16,
    ER_ILLEGAL_VALUE_FOR_TYPE = 1367u16,
    ER_VIEW_NONUPD_CHECK = 1368u16,
    ER_VIEW_CHECK_FAILED = 1369u16,
    ER_PROCACCESS_DENIED_ERROR = 1370u16,
    ER_RELAY_LOG_FAIL = 1371u16,
    ER_PASSWD_LENGTH = 1372u16,
    ER_UNKNOWN_TARGET_BINLOG = 1373u16,
    ER_IO_ERR_LOG_INDEX_READ = 1374u16,
    ER_BINLOG_PURGE_PROHIBITED = 1375u16,
    ER_FSEEK_FAIL = 1376u16,
    ER_BINLOG_PURGE_FATAL_ERR = 1377u16,
    ER_LOG_IN_USE = 1378u16,
    ER_LOG_PURGE_UNKNOWN_ERR = 1379u16,
    ER_RELAY_LOG_INIT = 1380u16,
    ER_NO_BINARY_LOGGING = 1381u16,
    ER_RESERVED_SYNTAX = 1382u16,
    ER_WSAS_FAILED = 1383u16,
    ER_DIFF_GROUPS_PROC = 1384u16,
    ER_NO_GROUP_FOR_PROC = 1385u16,
    ER_ORDER_WITH_PROC = 1386u16,
    ER_LOGGING_PROHIBIT_CHANGING_OF = 1387u16,
    ER_NO_FILE_MAPPING = 1388u16,
    ER_WRONG_MAGIC = 1389u16,
    ER_PS_MANY_PARAM = 1390u16,
    ER_KEY_PART_0 = 1391u16,
    ER_VIEW_CHECKSUM = 1392u16,
    ER_VIEW_MULTIUPDATE = 1393u16,
    ER_VIEW_NO_INSERT_FIELD_LIST = 1394u16,
    ER_VIEW_DELETE_MERGE_VIEW = 1395u16,
    ER_CANNOT_USER = 1396u16,
    ER_XAER_NOTA = 1397u16,
    ER_XAER_INVAL = 1398u16,
    ER_XAER_RMFAIL = 1399u16,
    ER_XAER_OUTSIDE = 1400u16,
    ER_XAER_RMERR = 1401u16,
    ER_XA_RBROLLBACK = 1402u16,
    ER_NONEXISTING_PROC_GRANT = 1403u16,
    ER_PROC_AUTO_GRANT_FAIL = 1404u16,
    ER_PROC_AUTO_REVOKE_FAIL = 1405u16,
    ER_DATA_TOO_LONG = 1406u16,
    ER_SP_BAD_SQLSTATE = 1407u16,
    ER_STARTUP = 1408u16,
    ER_LOAD_FROM_FIXED_SIZE_ROWS_TO_VAR = 1409u16,
    ER_CANT_CREATE_USER_WITH_GRANT = 1410u16,
    ER_WRONG_VALUE_FOR_TYPE = 1411u16,
    ER_TABLE_DEF_CHANGED = 1412u16,
    ER_SP_DUP_HANDLER = 1413u16,
    ER_SP_NOT_VAR_ARG = 1414u16,
    ER_SP_NO_RETSET = 1415u16,
    ER_CANT_CREATE_GEOMETRY_OBJECT = 1416u16,
    ER_FAILED_ROUTINE_BREAK_BINLOG = 1417u16,
    ER_BINLOG_UNSAFE_ROUTINE = 1418u16,
    ER_BINLOG_CREATE_ROUTINE_NEED_SUPER = 1419u16,
    ER_EXEC_STMT_WITH_OPEN_CURSOR = 1420u16,
    ER_STMT_HAS_NO_OPEN_CURSOR = 1421u16,
    ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG = 1422u16,
    ER_NO_DEFAULT_FOR_VIEW_FIELD = 1423u16,
    ER_SP_NO_RECURSION = 1424u16,
    ER_TOO_BIG_SCALE = 1425u16,
    ER_TOO_BIG_PRECISION = 1426u16,
    ER_M_BIGGER_THAN_D = 1427u16,
    ER_WRONG_LOCK_OF_SYSTEM_TABLE = 1428u16,
    ER_CONNECT_TO_FOREIGN_DATA_SOURCE = 1429u16,
    ER_QUERY_ON_FOREIGN_DATA_SOURCE = 1430u16,
    ER_FOREIGN_DATA_SOURCE_DOESNT_EXIST = 1431u16,
    ER_FOREIGN_DATA_STRING_INVALID_CANT_CREATE = 1432u16,
    ER_FOREIGN_DATA_STRING_INVALID = 1433u16,
    ER_CANT_CREATE_FEDERATED_TABLE = 1434u16,
    ER_TRG_IN_WRONG_SCHEMA = 1435u16,
    ER_STACK_OVERRUN_NEED_MORE = 1436u16,
    ER_TOO_LONG_BODY = 1437u16,
    ER_WARN_CANT_DROP_DEFAULT_KEYCACHE = 1438u16,
    ER_TOO_BIG_DISPLAYWIDTH = 1439u16,
    ER_XAER_DUPID = 1440u16,
    ER_DATETIME_FUNCTION_OVERFLOW = 1441u16,
    ER_CANT_UPDATE_USED_TABLE_IN_SF_OR_TRG = 1442u16,
    ER_VIEW_PREVENT_UPDATE = 1443u16,
    ER_PS_NO_RECURSION = 1444u16,
    ER_SP_CANT_SET_AUTOCOMMIT = 1445u16,
    ER_MALFORMED_DEFINER = 1446u16,
    ER_VIEW_FRM_NO_USER = 1447u16,
    ER_VIEW_OTHER_USER = 1448u16,
    ER_NO_SUCH_USER = 1449u16,
    ER_FORBID_SCHEMA_CHANGE = 1450u16,
    ER_ROW_IS_REFERENCED_2 = 1451u16,
    ER_NO_REFERENCED_ROW_2 = 1452u16,
    ER_SP_BAD_VAR_SHADOW = 1453u16,
    ER_TRG_NO_DEFINER = 1454u16,
    ER_OLD_FILE_FORMAT = 1455u16,
    ER_SP_RECURSION_LIMIT = 1456u16,
    ER_SP_PROC_TABLE_CORRUPT = 1457u16,
    ER_SP_WRONG_NAME = 1458u16,
    ER_TABLE_NEEDS_UPGRADE = 1459u16,
    ER_SP_NO_AGGREGATE = 1460u16,
    ER_MAX_PREPARED_STMT_COUNT_REACHED = 1461u16,
    ER_VIEW_RECURSIVE = 1462u16,
    ER_NON_GROUPING_FIELD_USED = 1463u16,
    ER_TABLE_CANT_HANDLE_SPKEYS = 1464u16,
    ER_NO_TRIGGERS_ON_SYSTEM_SCHEMA = 1465u16,
    ER_REMOVED_SPACES = 1466u16,
    ER_AUTOINC_READ_FAILED = 1467u16,
    ER_USERNAME = 1468u16,
    ER_HOSTNAME = 1469u16,
    ER_WRONG_STRING_LENGTH = 1470u16,
    ER_NON_INSERTABLE_TABLE = 1471u16,
    ER_ADMIN_WRONG_MRG_TABLE = 1472u16,
    ER_TOO_HIGH_LEVEL_OF_NESTING_FOR_SELECT = 1473u16,
    ER_NAME_BECOMES_EMPTY = 1474u16,
    ER_AMBIGUOUS_FIELD_TERM = 1475u16,
    ER_FOREIGN_SERVER_EXISTS = 1476u16,
    ER_FOREIGN_SERVER_DOESNT_EXIST = 1477u16,
    ER_ILLEGAL_HA_CREATE_OPTION = 1478u16,
    ER_PARTITION_REQUIRES_VALUES_ERROR = 1479u16,
    ER_PARTITION_WRONG_VALUES_ERROR = 1480u16,
    ER_PARTITION_MAXVALUE_ERROR = 1481u16,
    ER_PARTITION_SUBPARTITION_ERROR = 1482u16,
    ER_PARTITION_SUBPART_MIX_ERROR = 1483u16,
    ER_PARTITION_WRONG_NO_PART_ERROR = 1484u16,
    ER_PARTITION_WRONG_NO_SUBPART_ERROR = 1485u16,
    ER_CONST_EXPR_IN_PARTITION_FUNC_ERROR = 1486u16,
    ER_NO_CONST_EXPR_IN_RANGE_OR_LIST_ERROR = 1487u16,
    ER_FIELD_NOT_FOUND_PART_ERROR = 1488u16,
    ER_LIST_OF_FIELDS_ONLY_IN_HASH_ERROR = 1489u16,
    ER_INCONSISTENT_PARTITION_INFO_ERROR = 1490u16,
    ER_PARTITION_FUNC_NOT_ALLOWED_ERROR = 1491u16,
    ER_PARTITIONS_MUST_BE_DEFINED_ERROR = 1492u16,
    ER_RANGE_NOT_INCREASING_ERROR = 1493u16,
    ER_INCONSISTENT_TYPE_OF_FUNCTIONS_ERROR = 1494u16,
    ER_MULTIPLE_DEF_CONST_IN_LIST_PART_ERROR = 1495u16,
    ER_PARTITION_ENTRY_ERROR = 1496u16,
    ER_MIX_HANDLER_ERROR = 1497u16,
    ER_PARTITION_NOT_DEFINED_ERROR = 1498u16,
    ER_TOO_MANY_PARTITIONS_ERROR = 1499u16,
    ER_SUBPARTITION_ERROR = 1500u16,
    ER_CANT_CREATE_HANDLER_FILE = 1501u16,
    ER_BLOB_FIELD_IN_PART_FUNC_ERROR = 1502u16,
    ER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF = 1503u16,
    ER_NO_PARTS_ERROR = 1504u16,
    ER_PARTITION_MGMT_ON_NONPARTITIONED = 1505u16,
    ER_FOREIGN_KEY_ON_PARTITIONED = 1506u16,
    ER_DROP_PARTITION_NON_EXISTENT = 1507u16,
    ER_DROP_LAST_PARTITION = 1508u16,
    ER_COALESCE_ONLY_ON_HASH_PARTITION = 1509u16,
    ER_REORG_HASH_ONLY_ON_SAME_NO = 1510u16,
    ER_REORG_NO_PARAM_ERROR = 1511u16,
    ER_ONLY_ON_RANGE_LIST_PARTITION = 1512u16,
    ER_ADD_PARTITION_SUBPART_ERROR = 1513u16,
    ER_ADD_PARTITION_NO_NEW_PARTITION = 1514u16,
    ER_COALESCE_PARTITION_NO_PARTITION = 1515u16,
    ER_REORG_PARTITION_NOT_EXIST = 1516u16,
    ER_SAME_NAME_PARTITION = 1517u16,
    ER_NO_BINLOG_ERROR = 1518u16,
    ER_CONSECUTIVE_REORG_PARTITIONS = 1519u16,
    ER_REORG_OUTSIDE_RANGE = 1520u16,
    ER_PARTITION_FUNCTION_FAILURE = 1521u16,
    ER_PART_STATE_ERROR = 1522u16,
    ER_LIMITED_PART_RANGE = 1523u16,
    ER_PLUGIN_IS_NOT_LOADED = 1524u16,
    ER_WRONG_VALUE = 1525u16,
    ER_NO_PARTITION_FOR_GIVEN_VALUE = 1526u16,
    ER_FILEGROUP_OPTION_ONLY_ONCE = 1527u16,
    ER_CREATE_FILEGROUP_FAILED = 1528u16,
    ER_DROP_FILEGROUP_FAILED = 1529u16,
    ER_TABLESPACE_AUTO_EXTEND_ERROR = 1530u16,
    ER_WRONG_SIZE_NUMBER = 1531u16,
    ER_SIZE_OVERFLOW_ERROR = 1532u16,
    ER_ALTER_FILEGROUP_FAILED = 1533u16,
    ER_BINLOG_ROW_LOGGING_FAILED = 1534u16,
    ER_BINLOG_ROW_WRONG_TABLE_DEF = 1535u16,
    ER_BINLOG_ROW_RBR_TO_SBR = 1536u16,
    ER_EVENT_ALREADY_EXISTS = 1537u16,
    ER_EVENT_STORE_FAILED = 1538u16,
    ER_EVENT_DOES_NOT_EXIST = 1539u16,
    ER_EVENT_CANT_ALTER = 1540u16,
    ER_EVENT_DROP_FAILED = 1541u16,
    ER_EVENT_INTERVAL_NOT_POSITIVE_OR_TOO_BIG = 1542u16,
    ER_EVENT_ENDS_BEFORE_STARTS = 1543u16,
    ER_EVENT_EXEC_TIME_IN_THE_PAST = 1544u16,
    ER_EVENT_OPEN_TABLE_FAILED = 1545u16,
    ER_EVENT_NEITHER_M_EXPR_NOR_M_AT = 1546u16,
    ER_COL_COUNT_DOESNT_MATCH_CORRUPTED = 1547u16,
    ER_CANNOT_LOAD_FROM_TABLE = 1548u16,
    ER_EVENT_CANNOT_DELETE = 1549u16,
    ER_EVENT_COMPILE_ERROR = 1550u16,
    ER_EVENT_SAME_NAME = 1551u16,
    ER_EVENT_DATA_TOO_LONG = 1552u16,
    ER_DROP_INDEX_FK = 1553u16,
    ER_WARN_DEPRECATED_SYNTAX_WITH_VER = 1554u16,
    ER_CANT_WRITE_LOCK_LOG_TABLE = 1555u16,
    ER_CANT_LOCK_LOG_TABLE = 1556u16,
    ER_FOREIGN_DUPLICATE_KEY = 1557u16,
    ER_COL_COUNT_DOESNT_MATCH_PLEASE_UPDATE = 1558u16,
    ER_TEMP_TABLE_PREVENTS_SWITCH_OUT_OF_RBR = 1559u16,
    ER_STORED_FUNCTION_PREVENTS_SWITCH_BINLOG_FORMAT = 1560u16,
    ER_NDB_CANT_SWITCH_BINLOG_FORMAT = 1561u16,
    ER_PARTITION_NO_TEMPORARY = 1562u16,
    ER_PARTITION_CONST_DOMAIN_ERROR = 1563u16,
    ER_PARTITION_FUNCTION_IS_NOT_ALLOWED = 1564u16,
    ER_DDL_LOG_ERROR = 1565u16,
    ER_NULL_IN_VALUES_LESS_THAN = 1566u16,
    ER_WRONG_PARTITION_NAME = 1567u16,
    ER_CANT_CHANGE_TX_ISOLATION = 1568u16,
    ER_DUP_ENTRY_AUTOINCREMENT_CASE = 1569u16,
    ER_EVENT_MODIFY_QUEUE_ERROR = 1570u16,
    ER_EVENT_SET_VAR_ERROR = 1571u16,
    ER_PARTITION_MERGE_ERROR = 1572u16,
    ER_CANT_ACTIVATE_LOG = 1573u16,
    ER_RBR_NOT_AVAILABLE = 1574u16,
    ER_BASE64_DECODE_ERROR = 1575u16,
    ER_EVENT_RECURSION_FORBIDDEN = 1576u16,
    ER_EVENTS_DB_ERROR = 1577u16,
    ER_ONLY_INTEGERS_ALLOWED = 1578u16,
    ER_UNSUPORTED_LOG_ENGINE = 1579u16,
    ER_BAD_LOG_STATEMENT = 1580u16,
    ER_CANT_RENAME_LOG_TABLE = 1581u16,
    ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT = 1582u16,
    ER_WRONG_PARAMETERS_TO_NATIVE_FCT = 1583u16,
    ER_WRONG_PARAMETERS_TO_STORED_FCT = 1584u16,
    ER_NATIVE_FCT_NAME_COLLISION = 1585u16,
    ER_DUP_ENTRY_WITH_KEY_NAME = 1586u16,
    ER_BINLOG_PURGE_EMFILE = 1587u16,
    ER_EVENT_CANNOT_CREATE_IN_THE_PAST = 1588u16,
    ER_EVENT_CANNOT_ALTER_IN_THE_PAST = 1589u16,
    ER_SLAVE_INCIDENT = 1590u16,
    ER_NO_PARTITION_FOR_GIVEN_VALUE_SILENT = 1591u16,
    ER_BINLOG_UNSAFE_STATEMENT = 1592u16,
    ER_SLAVE_FATAL_ERROR = 1593u16,
    ER_SLAVE_RELAY_LOG_READ_FAILURE = 1594u16,
    ER_SLAVE_RELAY_LOG_WRITE_FAILURE = 1595u16,
    ER_SLAVE_CREATE_EVENT_FAILURE = 1596u16,
    ER_SLAVE_MASTER_COM_FAILURE = 1597u16,
    ER_BINLOG_LOGGING_IMPOSSIBLE = 1598u16,
    ER_VIEW_NO_CREATION_CTX = 1599u16,
    ER_VIEW_INVALID_CREATION_CTX = 1600u16,
    ER_SR_INVALID_CREATION_CTX = 1601u16,
    ER_TRG_CORRUPTED_FILE = 1602u16,
    ER_TRG_NO_CREATION_CTX = 1603u16,
    ER_TRG_INVALID_CREATION_CTX = 1604u16,
    ER_EVENT_INVALID_CREATION_CTX = 1605u16,
    ER_TRG_CANT_OPEN_TABLE = 1606u16,
    ER_CANT_CREATE_SROUTINE = 1607u16,
    ER_SLAVE_AMBIGOUS_EXEC_MODE = 1608u16,
    ER_NO_FORMAT_DESCRIPTION_EVENT_BEFORE_BINLOG_STATEMENT = 1609u16,
    ER_SLAVE_CORRUPT_EVENT = 1610u16,
    ER_LOAD_DATA_INVALID_COLUMN = 1611u16,
    ER_LOG_PURGE_NO_FILE = 1612u16,
    ER_XA_RBTIMEOUT = 1613u16,
    ER_XA_RBDEADLOCK = 1614u16,
    ER_NEED_REPREPARE = 1615u16,
    ER_DELAYED_NOT_SUPPORTED = 1616u16,
    WARN_NO_MASTER_INFO = 1617u16,
    WARN_OPTION_IGNORED = 1618u16,
    WARN_PLUGIN_DELETE_BUILTIN = 1619u16,
    WARN_PLUGIN_BUSY = 1620u16,
    ER_VARIABLE_IS_READONLY = 1621u16,
    ER_WARN_ENGINE_TRANSACTION_ROLLBACK = 1622u16,
    ER_SLAVE_HEARTBEAT_FAILURE = 1623u16,
    ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE = 1624u16,
    ER_NDB_REPLICATION_SCHEMA_ERROR = 1625u16,
    ER_CONFLICT_FN_PARSE_ERROR = 1626u16,
    ER_EXCEPTIONS_WRITE_ERROR = 1627u16,
    ER_TOO_LONG_TABLE_COMMENT = 1628u16,
    ER_TOO_LONG_FIELD_COMMENT = 1629u16,
    ER_FUNC_INEXISTENT_NAME_COLLISION = 1630u16,
    ER_DATABASE_NAME = 1631u16,
    ER_TABLE_NAME = 1632u16,
    ER_PARTITION_NAME = 1633u16,
    ER_SUBPARTITION_NAME = 1634u16,
    ER_TEMPORARY_NAME = 1635u16,
    ER_RENAMED_NAME = 1636u16,
    ER_TOO_MANY_CONCURRENT_TRXS = 1637u16,
    WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED = 1638u16,
    ER_DEBUG_SYNC_TIMEOUT = 1639u16,
    ER_DEBUG_SYNC_HIT_LIMIT = 1640u16,
    ER_DUP_SIGNAL_SET = 1641u16,
    ER_SIGNAL_WARN = 1642u16,
    ER_SIGNAL_NOT_FOUND = 1643u16,
    ER_SIGNAL_EXCEPTION = 1644u16,
    ER_RESIGNAL_WITHOUT_ACTIVE_HANDLER = 1645u16,
    ER_SIGNAL_BAD_CONDITION_TYPE = 1646u16,
    WARN_COND_ITEM_TRUNCATED = 1647u16,
    ER_COND_ITEM_TOO_LONG = 1648u16,
    ER_UNKNOWN_LOCALE = 1649u16,
    ER_SLAVE_IGNORE_SERVER_IDS = 1650u16,
    ER_QUERY_CACHE_DISABLED = 1651u16,
    ER_SAME_NAME_PARTITION_FIELD = 1652u16,
    ER_PARTITION_COLUMN_LIST_ERROR = 1653u16,
    ER_WRONG_TYPE_COLUMN_VALUE_ERROR = 1654u16,
    ER_TOO_MANY_PARTITION_FUNC_FIELDS_ERROR = 1655u16,
    ER_MAXVALUE_IN_VALUES_IN = 1656u16,
    ER_TOO_MANY_VALUES_ERROR = 1657u16,
    ER_ROW_SINGLE_PARTITION_FIELD_ERROR = 1658u16,
    ER_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD = 1659u16,
    ER_PARTITION_FIELDS_TOO_LONG = 1660u16,
    ER_BINLOG_ROW_ENGINE_AND_STMT_ENGINE = 1661u16,
    ER_BINLOG_ROW_MODE_AND_STMT_ENGINE = 1662u16,
    ER_BINLOG_UNSAFE_AND_STMT_ENGINE = 1663u16,
    ER_BINLOG_ROW_INJECTION_AND_STMT_ENGINE = 1664u16,
    ER_BINLOG_STMT_MODE_AND_ROW_ENGINE = 1665u16,
    ER_BINLOG_ROW_INJECTION_AND_STMT_MODE = 1666u16,
    ER_BINLOG_MULTIPLE_ENGINES_AND_SELF_LOGGING_ENGINE = 1667u16,
    ER_BINLOG_UNSAFE_LIMIT = 1668u16,
    ER_BINLOG_UNSAFE_INSERT_DELAYED = 1669u16,
    ER_BINLOG_UNSAFE_SYSTEM_TABLE = 1670u16,
    ER_BINLOG_UNSAFE_AUTOINC_COLUMNS = 1671u16,
    ER_BINLOG_UNSAFE_UDF = 1672u16,
    ER_BINLOG_UNSAFE_SYSTEM_VARIABLE = 1673u16,
    ER_BINLOG_UNSAFE_SYSTEM_FUNCTION = 1674u16,
    ER_BINLOG_UNSAFE_NONTRANS_AFTER_TRANS = 1675u16,
    ER_MESSAGE_AND_STATEMENT = 1676u16,
    ER_SLAVE_CONVERSION_FAILED = 1677u16,
    ER_SLAVE_CANT_CREATE_CONVERSION = 1678u16,
    ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_BINLOG_FORMAT = 1679u16,
    ER_PATH_LENGTH = 1680u16,
    ER_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT = 1681u16,
    ER_WRONG_NATIVE_TABLE_STRUCTURE = 1682u16,
    ER_WRONG_PERFSCHEMA_USAGE = 1683u16,
    ER_WARN_I_S_SKIPPED_TABLE = 1684u16,
    ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_BINLOG_DIRECT = 1685u16,
    ER_STORED_FUNCTION_PREVENTS_SWITCH_BINLOG_DIRECT = 1686u16,
    ER_SPATIAL_MUST_HAVE_GEOM_COL = 1687u16,
    ER_TOO_LONG_INDEX_COMMENT = 1688u16,
    ER_LOCK_ABORTED = 1689u16,
    ER_DATA_OUT_OF_RANGE = 1690u16,
    ER_WRONG_SPVAR_TYPE_IN_LIMIT = 1691u16,
    ER_BINLOG_UNSAFE_MULTIPLE_ENGINES_AND_SELF_LOGGING_ENGINE = 1692u16,
    ER_BINLOG_UNSAFE_MIXED_STATEMENT = 1693u16,
    ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_SQL_LOG_BIN = 1694u16,
    ER_STORED_FUNCTION_PREVENTS_SWITCH_SQL_LOG_BIN = 1695u16,
    ER_FAILED_READ_FROM_PAR_FILE = 1696u16,
    ER_VALUES_IS_NOT_INT_TYPE_ERROR = 1697u16,
    ER_ACCESS_DENIED_NO_PASSWORD_ERROR = 1698u16,
    ER_SET_PASSWORD_AUTH_PLUGIN = 1699u16,
    ER_GRANT_PLUGIN_USER_EXISTS = 1700u16,
    ER_TRUNCATE_ILLEGAL_FK = 1701u16,
    ER_PLUGIN_IS_PERMANENT = 1702u16,
    ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MIN = 1703u16,
    ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MAX = 1704u16,
    ER_STMT_CACHE_FULL = 1705u16,
    ER_MULTI_UPDATE_KEY_CONFLICT = 1706u16,
    ER_TABLE_NEEDS_REBUILD = 1707u16,
    WARN_OPTION_BELOW_LIMIT = 1708u16,
    ER_INDEX_COLUMN_TOO_LONG = 1709u16,
    ER_ERROR_IN_TRIGGER_BODY = 1710u16,
    ER_ERROR_IN_UNKNOWN_TRIGGER_BODY = 1711u16,
    ER_INDEX_CORRUPT = 1712u16,
    ER_UNDO_RECORD_TOO_BIG = 1713u16,
    ER_BINLOG_UNSAFE_INSERT_IGNORE_SELECT = 1714u16,
    ER_BINLOG_UNSAFE_INSERT_SELECT_UPDATE = 1715u16,
    ER_BINLOG_UNSAFE_REPLACE_SELECT = 1716u16,
    ER_BINLOG_UNSAFE_CREATE_IGNORE_SELECT = 1717u16,
    ER_BINLOG_UNSAFE_CREATE_REPLACE_SELECT = 1718u16,
    ER_BINLOG_UNSAFE_UPDATE_IGNORE = 1719u16,
    ER_PLUGIN_NO_UNINSTALL = 1720u16,
    ER_PLUGIN_NO_INSTALL = 1721u16,
    ER_BINLOG_UNSAFE_WRITE_AUTOINC_SELECT = 1722u16,
    ER_BINLOG_UNSAFE_CREATE_SELECT_AUTOINC = 1723u16,
    ER_BINLOG_UNSAFE_INSERT_TWO_KEYS = 1724u16,
    ER_TABLE_IN_FK_CHECK = 1725u16,
    ER_UNSUPPORTED_ENGINE = 1726u16,
    ER_BINLOG_UNSAFE_AUTOINC_NOT_FIRST = 1727u16,
}
