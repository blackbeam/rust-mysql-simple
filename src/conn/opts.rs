#[cfg(any(feature = "socket", feature = "pipe"))]
use std::net::{Ipv4Addr, Ipv6Addr};

#[cfg(any(feature = "socket", feature = "ssl"))]
use std::path;

#[cfg(any(feature = "socket", feature = "pipe"))]
use std::str::FromStr;

use std::time::Duration;

use super::super::error::UrlError;
use super::LocalInfileHandler;

use url::Url;
use url::percent_encoding::percent_decode;


/// Ssl options: Option<(ca_cert, Option<(client_cert, client_key)>)>.`
#[cfg(feature = "ssl")]
pub type SslOpts = Option<(path::PathBuf, Option<(path::PathBuf, path::PathBuf)>)>;

/// Mysql connection options.
///
/// Build one with [`OptsBuilder`](struct.OptsBuilder.html).
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Opts {
    /// Address of mysql server (defaults to `127.0.0.1`). Hostnames should also work.
    ip_or_hostname: Option<String>,
    /// TCP port of mysql server (defaults to `3306`).
    tcp_port: u16,
    /// Path to unix socket of mysql server (defaults to `None`).
    #[cfg(feature = "socket")]
    unix_addr: Option<path::PathBuf>,
    /// Pipe name of mysql server (defaults to `None`).
    #[cfg(feature = "pipe")]
    pipe_name: Option<String>,
    /// User (defaults to `None`).
    user: Option<String>,
    /// Password (defaults to `None`).
    pass: Option<String>,
    /// Database name (defaults to `None`).
    db_name: Option<String>,

    /// The timeout for each attempt to read from the server.
    read_timeout: Option<Duration>,

    /// The timeout for each attempt to write to the server.
    write_timeout: Option<Duration>,

    #[cfg(any(feature = "socket", feature = "pipe"))]
    /// Prefer socket connection (defaults to `true`).
    ///
    /// Will reconnect via socket after TCP connection to `127.0.0.1` if `true`.
    prefer_socket: bool,
    // XXX: Wait for keepalive_timeout stabilization
    /// Commands to execute on each new database connection.
    init: Vec<String>,

    #[cfg(feature = "ssl")]
    /// #### Only available if `ssl` feature enabled.
    /// Perform or not ssl peer verification (defaults to `false`).
    /// Only make sense if ssl_opts is not None.
    verify_peer: bool,

    #[cfg(feature = "ssl")]
    /// #### Only available if `ssl` feature enabled.
    /// SSL certificates and keys in pem format.
    /// If not None, then ssl connection implied.
    ///
    /// `Option<(ca_cert, Option<(client_cert, client_key)>)>.`
    ssl_opts: SslOpts,

    /// Callback to handle requests for local files. These are
    /// caused by using `LOAD DATA LOCAL INFILE` queries. The
    /// callback is passed the filename, and a `Write`able object
    /// to receive the contents of that file.
    /// If unset, the default callback will read files relative to
    /// the current directory.
    local_infile_handler: Option<LocalInfileHandler>
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
                addr.is_loopback()
            } else if let Some(addr) = v6addr {
                addr.is_loopback()
            } else if self.ip_or_hostname.as_ref().unwrap() == "localhost" {
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn from_url(url: &str) -> Result<Opts, UrlError> {
        from_url(url)
    }

    /// Address of mysql server (defaults to `127.0.0.1`). Hostnames should also work.
    pub fn get_ip_or_hostname(&self) -> &Option<String> {
        &self.ip_or_hostname
    }
    /// TCP port of mysql server (defaults to `3306`).
    pub fn get_tcp_port(&self) -> u16 {
        self.tcp_port
    }
    /// Path to unix socket of mysql server (defaults to `None`).
    #[cfg(feature = "socket")]
    pub fn get_unix_addr(&self) -> &Option<path::PathBuf> {
        &self.unix_addr
    }
    /// Pipe name of mysql server (defaults to `None`).
    #[cfg(feature = "pipe")]
    pub fn get_pipe_name(&self) -> &Option<String> {
        &self.pipe_name
    }
    /// User (defaults to `None`).
    pub fn get_user(&self) -> &Option<String> {
        &self.user
    }
    /// Password (defaults to `None`).
    pub fn get_pass(&self) -> &Option<String> {
        &self.pass
    }
    /// Database name (defaults to `None`).
    pub fn get_db_name(&self) -> &Option<String> {
        &self.db_name
    }

    /// The timeout for each attempt to write to the server.
    pub fn get_read_timeout(&self) -> &Option<Duration> {
        &self.read_timeout
    }

    /// The timeout for each attempt to write to the server.
    pub fn get_write_timeout(&self) -> &Option<Duration> {
        &self.write_timeout
    }

    #[cfg(any(feature = "socket", feature = "pipe"))]
    /// Prefer socket connection (defaults to `true`).
    ///
    /// Will reconnect via socket after TCP connection to `127.0.0.1` if `true`.
    pub fn get_prefer_socket(&self) -> bool {
        self.prefer_socket
    }
    // XXX: Wait for keepalive_timeout stabilization
    /// Commands to execute on each new database connection.
    pub fn get_init(&self) -> &Vec<String> {
        &self.init
    }

    #[cfg(feature = "ssl")]
    /// #### Only available if `ssl` feature enabled.
    /// Perform or not ssl peer verification (defaults to `false`).
    /// Only make sense if ssl_opts is not None.
    pub fn get_verify_peer(&self) -> bool {
        self.verify_peer
    }

    #[cfg(feature = "ssl")]
    /// #### Only available if `ssl` feature enabled.
    /// SSL certificates and keys in pem format.
    /// If not None, then ssl connection implied.
    ///
    /// `Option<(ca_cert, Option<(client_cert, client_key)>)>.`
    pub fn get_ssl_opts(&self) -> &SslOpts {
        &self.ssl_opts
    }

    #[cfg(any(feature = "socket", feature = "pipe"))]
    fn set_prefer_socket(&mut self, val: bool) {
        self.prefer_socket = val;
    }

    #[allow(unused_variables)]
    #[cfg(all(not(feature = "socket"), not(feature = "pipe")))]
    fn set_prefer_socket(&mut self, val: bool) {
        ()
    }

    #[cfg(feature = "ssl")]
    fn set_verify_peer(&mut self, val: bool) {
        self.verify_peer = val;
    }

    #[allow(unused_variables)]
    #[cfg(not(feature = "ssl"))]
    fn set_verify_peer(&mut self, val: bool) {
        ()
    }

    /// Callback to handle requests for local files.
    pub fn get_local_infile_handler(&self) -> &Option<LocalInfileHandler> {
        &self.local_infile_handler
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
            read_timeout: None,
            write_timeout: None,
            prefer_socket: true,
            init: vec![],
            local_infile_handler: None
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
            read_timeout: None,
            write_timeout: None,
            init: vec![],
            local_infile_handler: None
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
            read_timeout: None,
            write_timeout: None,
            prefer_socket: true,
            init: vec![],
            local_infile_handler: None
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
            read_timeout: None,
            write_timeout: None,
            init: vec![],
            verify_peer: false,
            ssl_opts: None,
            local_infile_handler: None
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
            read_timeout: None,
            write_timeout: None,
            init: vec![],
            verify_peer: false,
            prefer_socket: true,
            ssl_opts: None,
            local_infile_handler: None
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
            read_timeout: None,
            write_timeout: None,
            prefer_socket: true,
            init: vec![],
            verify_peer: false,
            ssl_opts: None,
            local_infile_handler: None
        }
    }
}

/// Provides a way to build [`Opts`](struct.Opts.html).
///
/// ```ignore
/// // You can create new default builder
/// let mut builder = OptsBuilder::new();
/// builder.ip_or_hostname(Some("foo"))
///        .db_name(Some("bar"))
///        .ssl_opts(Some(("/foo/cert.pem", None::<(String, String)>)));
///
/// // Or use existing T: Into<Opts>
/// let mut builder = OptsBuilder::from_opts(existing_opts);
/// builder.ip_or_hostname(Some("foo"))
///        .db_name(Some("bar"));
/// ```
pub struct OptsBuilder {
    opts: Opts,
}

impl OptsBuilder {
    pub fn new() -> Self {
        OptsBuilder::default()
    }

    pub fn from_opts<T: Into<Opts>>(opts: T) -> Self {
        OptsBuilder {
            opts: opts.into(),
        }
    }

    /// Address of mysql server (defaults to `127.0.0.1`). Hostnames should also work.
    pub fn ip_or_hostname<T: Into<String>>(&mut self, ip_or_hostname: Option<T>) -> &mut Self {
        self.opts.ip_or_hostname = ip_or_hostname.map(Into::into);
        self
    }

    /// TCP port of mysql server (defaults to `3306`).
    pub fn tcp_port(&mut self, tcp_port: u16) -> &mut Self {
        self.opts.tcp_port = tcp_port;
        self
    }

    /// Path to unix socket of mysql server (defaults to `None`).
    #[cfg(feature = "socket")]
    pub fn unix_addr<T: Into<path::PathBuf>>(&mut self, unix_addr: Option<T>) -> &mut Self {
        self.opts.unix_addr = unix_addr.map(Into::into);
        self
    }

    /// Pipe name of mysql server (defaults to `None`).
    #[cfg(feature = "pipe")]
    pub fn pipe_name<T: Into<String>>(&mut self, pipe_name: Option<T>) -> &mut Self {
        self.opts.pipe_name = pipe_name.map(Into::into);
        self
    }

    /// User (defaults to `None`).
    pub fn user<T: Into<String>>(&mut self, user: Option<T>) -> &mut Self {
        self.opts.user = user.map(Into::into);
        self
    }

    /// Password (defaults to `None`).
    pub fn pass<T: Into<String>>(&mut self, pass: Option<T>) -> &mut Self {
        self.opts.pass = pass.map(Into::into);
        self
    }

    /// Database name (defaults to `None`).
    pub fn db_name<T: Into<String>>(&mut self, db_name: Option<T>) -> &mut Self {
        self.opts.db_name = db_name.map(Into::into);
        self
    }

    /// The timeout for each attempt to read from the server (defaults to `None`).
    ///
    /// Note that named pipe connection will ignore duration's `nanos`, and also note that
    /// it is an error to pass the zero `Duration` to this method.
    pub fn read_timeout(&mut self, read_timeout: Option<Duration>) -> &mut Self {
        self.opts.read_timeout = read_timeout;
        self
    }

    /// The timeout for each attempt to write to the server (defaults to `None`).
    ///
    /// Note that named pipe connection will ignore duration's `nanos`, and also note that
    /// it is likely error to pass the zero `Duration` to this method.
    pub fn write_timeout(&mut self, write_timeout: Option<Duration>) -> &mut Self {
        self.opts.write_timeout = write_timeout;
        self
    }

    #[cfg(any(feature = "socket", feature = "pipe"))]
    /// Prefer socket connection (defaults to `true`).
    ///
    /// Will reconnect via socket after TCP connection to `127.0.0.1` if `true`.
    pub fn prefer_socket(&mut self, prefer_socket: bool) -> &mut Self {
        self.opts.prefer_socket = prefer_socket;
        self
    }

    // XXX: Wait for keepalive_timeout stabilization
    /// Commands to execute on each new database connection.
    pub fn init<T: Into<String>>(&mut self, init: Vec<T>) -> &mut Self {
        self.opts.init = init.into_iter().map(Into::into).collect();
        self
    }

    #[cfg(feature = "ssl")]
    /// #### Only available if `ssl` feature enabled.
    /// Perform or not ssl peer verification (defaults to `false`).
    /// Only make sense if ssl_opts is not None.
    pub fn verify_peer(&mut self, verify_peer: bool) -> &mut Self {
        self.opts.verify_peer = verify_peer;
        self
    }

    #[cfg(feature = "ssl")]
    /// #### Only available if `ssl` feature enabled.
    /// SSL certificates and keys in pem format.
    /// If not None, then ssl connection implied.
    ///
    /// `Option<(ca_cert, Option<(client_cert, client_key)>)>.`
    pub fn ssl_opts<A, B, C>(&mut self, ssl_opts: Option<(A, Option<(B, C)>)>) -> &mut Self
    where A: Into<path::PathBuf>,
          B: Into<path::PathBuf>,
          C: Into<path::PathBuf> {
        self.opts.ssl_opts = ssl_opts.map(|(ca_cert, rest)| {
            (ca_cert.into(), rest.map(|(client_cert, client_key)| {
                (client_cert.into(), client_key.into())
            }))
        });
        self
    }

    /// Callback to handle requests for local files. These are
    /// caused by using `LOAD DATA LOCAL INFILE` queries. The
    /// callback is passed the filename, and a `Write`able object
    /// to receive the contents of that file.
    /// If unset, the default callback will read files relative to
    /// the current directory.
    pub fn local_infile_handler(&mut self, handler: LocalInfileHandler) -> &mut Self {
        self.opts.local_infile_handler = Some(handler);
        self
    }
}

impl From<OptsBuilder> for Opts {
    fn from(builder: OptsBuilder) -> Opts {
        builder.opts
    }
}

impl Default for OptsBuilder {
    fn default() -> OptsBuilder {
        OptsBuilder {
            opts: Opts::default(),
        }
    }
}

fn get_opts_user_from_url(url: &Url) -> Option<String> {
    let user = url.username();
    if user != "" {
        Some(percent_decode(user.as_ref()).decode_utf8_lossy().into_owned())
    } else {
        None
    }
}

fn get_opts_pass_from_url(url: &Url) -> Option<String> {
    if let Some(pass) = url.password() {
        Some(percent_decode(pass.as_ref()).decode_utf8_lossy().into_owned())
    } else {
        None
    }
}

fn get_opts_db_name_from_url(url: &Url) -> Option<String> {
    if let Some(mut segments) = url.path_segments() {
        segments.next().map(|db_name| {
            percent_decode(db_name.as_ref()).decode_utf8_lossy().into_owned()
        })
    } else {
        None
    }
}

fn from_url_basic(url_str: &str) -> Result<(Opts, Vec<(String, String)>), UrlError> {
    let url = try!(Url::parse(url_str));
    if url.scheme() != "mysql" {
        return Err(UrlError::UnsupportedScheme(url.scheme().to_string()));
    }
    if url.cannot_be_a_base() || !url.has_host() {
        return Err(UrlError::BadUrl);
    }
    let user = get_opts_user_from_url(&url);
    let pass = get_opts_pass_from_url(&url);
    let ip_or_hostname = url.host_str().map(String::from);
    let tcp_port = url.port().unwrap_or(3306);
    let db_name = get_opts_db_name_from_url(&url);

    let query_pairs = url.query_pairs().into_owned().collect();
    let opts = Opts {
        user: user,
        pass: pass,
        ip_or_hostname: ip_or_hostname,
        tcp_port: tcp_port,
        db_name: db_name,
        ..Opts::default()
    };

    Ok((opts, query_pairs))
}

fn from_url(url: &str) -> Result<Opts, UrlError> {
    let (mut opts, query_pairs) = try!(from_url_basic(url));
    for (key, value) in query_pairs {
        if key == "prefer_socket" {
            if cfg!(all(not(feature = "socket"), not(feature = "pipe"))) {
                return Err(
                    UrlError::FeatureRequired("`socket' or `pipe'".into(), "prefer_socket".into())
                );
            } else {
                if value == "true" {
                    opts.set_prefer_socket(true);
                } else if value == "false" {
                    opts.set_prefer_socket(false);
                } else {
                    return Err(UrlError::InvalidValue("prefer_socket".into(), value));
                }
            }
        } else if key == "verify_peer" {
            if cfg!(not(feature = "ssl")) {
                return Err(UrlError::FeatureRequired("`ssl'".into(), "verify_peer".into()));
            } else {
                if value == "true" {
                    opts.set_verify_peer(true);
                } else if value == "false" {
                    opts.set_verify_peer(false);
                } else {
                    return Err(UrlError::InvalidValue("verify_peer".into(), value));
                }
            }
        } else {
            return Err(UrlError::UnknownParameter(key));
        }
    }
    Ok(opts)
}

impl<'a> From<&'a str> for Opts {
    fn from(url: &'a str) -> Opts {
        match from_url(url) {
            Ok(opts) => opts,
            Err(err) => panic!("{}", err),
        }
    }
}

#[cfg(test)]
mod test {
    use super::Opts;

    #[test]
    #[cfg(all(feature = "ssl", feature = "socket"))]
    fn should_convert_url_into_opts() {
        let opts = "mysql://us%20r:p%20w@localhost:3308/db%2dname?prefer_socket=false&verify_peer=true";
        assert_eq!(Opts {
            user: Some("us r".to_string()),
            pass: Some("p w".to_string()),
            ip_or_hostname: Some("localhost".to_string()),
            tcp_port: 3308,
            db_name: Some("db-name".to_string()),
            prefer_socket: false,
            verify_peer: true,
            ..Opts::default()
        }, opts.into());
    }

    #[test]
    #[cfg(all(not(feature = "ssl"), not(feature = "socket")))]
    fn should_convert_url_into_opts() {
        let opts = "mysql://usr:pw@192.168.1.1:3309/dbname";
        assert_eq!(Opts {
            user: Some("usr".to_string()),
            pass: Some("pw".to_string()),
            ip_or_hostname: Some("192.168.1.1".to_string()),
            tcp_port: 3309,
            db_name: Some("dbname".to_string()),
            ..Opts::default()
        }, opts.into());
    }

    #[test]
    #[should_panic]
    fn should_panic_on_invalid_url() {
        let opts = "42";
        let _: Opts = opts.into();
    }

    #[test]
    #[should_panic]
    fn should_panic_on_invalid_scheme() {
        let opts = "postgres://localhost";
        let _: Opts = opts.into();
    }

    #[test]
    #[should_panic]
    fn should_panic_on_unknown_query_param() {
        let opts = "mysql://localhost/foo?bar=baz";
        let _: Opts = opts.into();
    }

    #[test]
    #[should_panic]
    #[cfg(all(not(feature = "socket"), not(feature = "pipe")))]
    fn should_panic_if_prefer_socket_query_param_requires_feature() {
        let opts = "mysql://usr:pw@localhost:3308/dbname?prefer_socket=false";
        let _: Opts = opts.into();
    }

    #[test]
    #[should_panic]
    #[cfg(not(feature = "ssl"))]
    fn should_panic_if_verify_peer_query_param_requires_feature() {
        let opts = "mysql://usr:pw@localhost:3308/dbname?verify_peer=false";
        let _: Opts = opts.into();
    }

    #[test]
    #[should_panic]
    #[cfg(feature = "socket")]
    fn should_panic_on_invalid_prefer_socket_param_value() {
        let opts = "mysql://usr:pw@localhost:3308/dbname?prefer_socket=invalid";
        let _: Opts = opts.into();
    }

    #[test]
    #[should_panic]
    #[cfg(feature = "ssl")]
    fn should_panic_on_invalid_verify_peer_param_value() {
        let opts = "mysql://usr:pw@localhost:3308/dbname?verify_peer=invalid";
        let _: Opts = opts.into();
    }

    #[test]
    #[should_panic]
    #[cfg(all(not(feature = "ssl"), not(feature = "socket")))]
    fn should_panic_on_unk() {
        let opts = "mysql://localhost/dbname?prefer_socket=false";
        assert_eq!(Opts {
            user: Some("usr".to_string()),
            pass: Some("pw".to_string()),
            ip_or_hostname: Some("localhost".to_string()),
            tcp_port: 3308,
            db_name: Some("dbname".to_string()),
            ..Opts::default()
        }, opts.into());
    }
}
