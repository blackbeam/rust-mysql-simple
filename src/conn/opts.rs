#[cfg(any(feature = "socket", feature = "pipe"))]
use std::net::{Ipv4Addr, Ipv6Addr};

#[cfg(feature = "socket")]
use std::path;

#[cfg(any(feature = "socket", feature = "pipe"))]
use std::str::FromStr;

/// Mysql connection options.
///
/// For example:
///
/// ```ignore
/// let opts = Opts {
///     user: Some("username".to_string()),
///     pass: Some("password".to_string()),
///     db_name: Some("mydatabase".to_string()),
///     ..Default::default()
/// };
/// ```
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Opts {
    /// Address of mysql server (defaults to `127.0.0.1`). Hostnames should also work.
    pub ip_or_hostname: Option<String>,
    /// TCP port of mysql server (defaults to `3306`).
    pub tcp_port: u16,
    /// Path to unix socket of mysql server (defaults to `None`).
    #[cfg(feature = "socket")]
    pub unix_addr: Option<path::PathBuf>,
    /// Pipe name of mysql server (defaults to `None`).
    #[cfg(feature = "pipe")]
    pub pipe_name: Option<String>,
    /// User (defaults to `None`).
    pub user: Option<String>,
    /// Password (defaults to `None`).
    pub pass: Option<String>,
    /// Database name (defaults to `None`).
    pub db_name: Option<String>,

    #[cfg(any(feature = "socket", feature = "pipe"))]
    /// Prefer socket connection (defaults to `true`).
    ///
    /// Will reconnect via socket after TCP connection to `127.0.0.1` if `true`.
    pub prefer_socket: bool,
    // XXX: Wait for keepalive_timeout stabilization
    /// Commands to execute on each new database connection.
    pub init: Vec<String>,

    #[cfg(feature = "ssl")]
    /// #### Only available if `ssl` feature enabled.
    /// Perform or not ssl peer verification (defaults to `false`).
    /// Only make sense if ssl_opts is not None.
    pub verify_peer: bool,

    #[cfg(feature = "ssl")]
    /// #### Only available if `ssl` feature enabled.
    /// SSL certificates and keys in pem format.
    /// If not None, then ssl connection implied.
    ///
    /// `Option<(ca_cert, Option<(client_cert, client_key)>)>.`
    pub ssl_opts: Option<(path::PathBuf, Option<(path::PathBuf, path::PathBuf)>)>
}

impl Opts {
    #[doc(hidden)]
    #[cfg(any(feature = "socket", feature = "pipe"))]
    pub fn addr_is_loopback(&self) -> bool {
        if self.ip_or_hostname.is_some() {
            let v4addr: Option<Ipv4Addr> = FromStr::from_str(
                self.ip_or_hostname.as_ref().unwrap().as_ref()).ok();
            let v6addr: Option<Ipv6Addr> = FromStr::from_str(
                self.ip_or_hostname.as_ref().unwrap().as_ref()).ok();
            if let Some(addr) = v4addr {
                addr.octets()[0] == 127
            } else if let Some(addr) = v6addr {
                addr.segments() == [0, 0, 0, 0, 0, 0, 0, 1]
            } else if self.ip_or_hostname.as_ref().unwrap() == "localhost" {
                true
            } else {
                false
            }
        } else {
            false
        }
    }
}

#[cfg(all(not(feature = "ssl"), feature = "socket", not(feature = "pipe")))]
impl Default for Opts {
    fn default() -> Opts {
        Opts {
            ip_or_hostname: Some("127.0.0.1".to_string()),
            tcp_port: 3306,
            unix_addr: None,
            user: None,
            pass: None,
            db_name: None,
            prefer_socket: true,
            init: vec![],
        }
    }
}

#[cfg(all(not(feature = "ssl"), not(feature = "socket"), not(feature = "pipe")))]
impl Default for Opts {
    fn default() -> Opts {
        Opts {
            ip_or_hostname: Some("127.0.0.1".to_string()),
            tcp_port: 3306,
            user: None,
            pass: None,
            db_name: None,
            init: vec![],
        }
    }
}

#[cfg(all(not(feature = "ssl"), not(feature = "socket"), feature = "pipe"))]
impl Default for Opts {
    fn default() -> Opts {
        Opts {
            ip_or_hostname: Some("127.0.0.1".to_string()),
            tcp_port: 3306,
            pipe_name: None,
            user: None,
            pass: None,
            db_name: None,
            prefer_socket: true,
            init: vec![],
        }
    }
}

#[cfg(all(feature = "ssl", not(feature = "socket"), not(feature = "pipe")))]
impl Default for Opts {
    fn default() -> Opts {
        Opts {
            ip_or_hostname: Some("127.0.0.1".to_string()),
            tcp_port: 3306,
            user: None,
            pass: None,
            db_name: None,
            init: vec![],
            verify_peer: false,
            ssl_opts: None,
        }
    }
}

#[cfg(all(feature = "ssl", not(feature = "socket"), feature = "pipe"))]
impl Default for Opts {
    fn default() -> Opts {
        Opts {
            ip_or_hostname: Some("127.0.0.1".to_string()),
            tcp_port: 3306,
            pipe_name: None,
            user: None,
            pass: None,
            db_name: None,
            init: vec![],
            verify_peer: false,
            prefer_socket: true,
            ssl_opts: None,
        }
    }
}

#[cfg(all(feature = "ssl", feature = "socket", not(feature = "pipe")))]
impl Default for Opts {
    fn default() -> Opts {
        Opts {
            ip_or_hostname: Some("127.0.0.1".to_string()),
            tcp_port: 3306,
            unix_addr: None,
            user: None,
            pass: None,
            db_name: None,
            prefer_socket: true,
            init: vec![],
            verify_peer: false,
            ssl_opts: None,
        }
    }
}
