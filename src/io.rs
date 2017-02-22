use std::io;
use std::io::Read as StdRead;
use std::io::Write as StdWrite;
use std::net;
use std::net::ToSocketAddrs;
use std::fmt;
use std::time::Duration;

#[cfg(all(feature = "ssl", not(target_os = "windows")))]
use conn::SslOpts;

use super::value::Value;
use super::value::Value::{NULL, Int, UInt, Float, Bytes, Date, Time};
use super::consts;
use super::consts::Command;
use super::consts::ColumnType;
use super::error::Error::DriverError;
use super::error::DriverError::ConnectTimeout;
use super::error::DriverError::CouldNotConnect;
use super::error::DriverError::PacketTooLarge;
use super::error::DriverError::PacketOutOfSync;
use super::error::Result as MyResult;

#[cfg(all(feature = "ssl", all(unix, not(target_os = "macos"))))]
use openssl::x509;
#[cfg(all(feature = "ssl", all(unix, not(target_os = "macos"))))]
use openssl::ssl::{self, SslStream, SslContext};
#[cfg(all(feature = "ssl", target_os = "macos"))]
use security_framework::secure_transport::{
    ConnectionType,
    HandshakeError,
    ProtocolSide,
    SslContext,
    SslStream,
};
#[cfg(all(feature = "ssl", target_os = "macos"))]
use security_framework::certificate::SecCertificate;
#[cfg(all(feature = "ssl", target_os = "macos"))]
use security_framework::cipher_suite::CipherSuite;
#[cfg(all(feature = "ssl", target_os = "macos"))]
use security_framework::identity::SecIdentity;
use bufstream::BufStream;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use byteorder::LittleEndian as LE;
#[cfg(unix)]
use std::os::unix as unix;
#[cfg(windows)]
use named_pipe as np;
use net2::TcpStreamExt;

#[inline(always)]
fn connect_tcp_stream<T: ToSocketAddrs>(address: T) -> io::Result<net::TcpStream> {
    net::TcpStream::connect(address)
}

use self::connect_timeout::connect_tcp_stream_timeout;

#[cfg(not(unix))]
mod connect_timeout {
    use std::io;
    use std::net::ToSocketAddrs;
    use std::net::TcpStream;
    use std::time::Duration;

    pub fn connect_tcp_stream_timeout<T>(_: T, _: Duration) -> io::Result<TcpStream>
        where T: ToSocketAddrs,
    {
        panic!("tcp_connect_timeout is unix-only feature");
    }
}

#[cfg(unix)]
mod connect_timeout {
    use libc::*;
    use net2::TcpBuilder;
    use nix;
    use std::io;
    use std::net::TcpStream;
    use std::net::ToSocketAddrs;
    use std::os::unix::prelude::*;
    use std::time::Duration;

    fn set_non_blocking(fd: RawFd, non_blocking: bool) -> io::Result<()> {
        let result = unsafe {
            ioctl(fd, FIONBIO, &mut (non_blocking as c_ulong))
        };

        if result == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn connect_tcp_stream_timeout<T>(address: T, timeout: Duration) -> io::Result<TcpStream>
        where T: ToSocketAddrs
    {
        let builder = TcpBuilder::new_v4()?;
        let fd = builder.as_raw_fd();

        set_non_blocking(fd, true)?;

        let err = io::Error::new(io::ErrorKind::Other,
                                 "no socket addresses resolved");
        let addresses = address.to_socket_addrs()?;
        addresses.fold(Err(err), |prev, addr| {
            let inet_addr = nix::sys::socket::InetAddr::from_std(&addr);
            let sock_addr = nix::sys::socket::SockAddr::Inet(inet_addr);
            prev.or_else(|_| {
                match nix::sys::socket::connect(fd, &sock_addr) {
                    Ok(_) => Ok(()),
                    Err(err) => {
                        match err.errno() {
                            nix::errno::Errno::EALREADY |
                            nix::errno::Errno::EINPROGRESS => Ok(()),
                            errno => {
                                Err(io::Error::from_raw_os_error(errno as i32))
                            },
                        }
                    }
                }
            })
        })?;

        let mut fd_set = nix::sys::select::FdSet::new();
        let socket_fd = fd;
        fd_set.insert(socket_fd);
        let mut timeout_timeval = nix::sys::time::TimeVal::microseconds(
            timeout.as_secs() as i64 * 1000 * 1000);

        let select_res = nix::sys::select::select(
            socket_fd + 1,
            None,
            Some(&mut fd_set),
            None,
            Some(&mut timeout_timeval),
        )?;

        if select_res == -1 {
            return Err(io::Error::last_os_error());
        }

        if select_res != 1 {
            return Err(io::ErrorKind::TimedOut.into());
        }

        let socket_error_code = nix::sys::socket::getsockopt(
            socket_fd,
            nix::sys::socket::sockopt::SocketError,
        )?;

        if socket_error_code != 0 {
            return Err(io::Error::from_raw_os_error(socket_error_code));
        }

        set_non_blocking(fd, false)?;

        builder.to_tcp_stream()
    }
}

pub trait Read: ReadBytesExt + io::BufRead {
    fn read_lenenc_int(&mut self) -> io::Result<u64> {
        let head_byte = try!(self.read_u8());
        let length = match head_byte {
            0xfc => 2,
            0xfd => 3,
            0xfe => 8,
            x => return Ok(x as u64),
        };
        let out = try!(self.read_uint::<LE>(length));
        Ok(out)
    }

    fn read_lenenc_bytes(&mut self) -> io::Result<Vec<u8>> {
        let len = try!(self.read_lenenc_int());
        let mut out = Vec::with_capacity(len as usize);
        let count = if len > 0 {
            try!(self.take(len).read_to_end(&mut out))
        } else {
            0
        };
        if count as u64 == len {
            Ok(out)
        } else {
            Err(io::Error::new(io::ErrorKind::Other,
                               "Unexpected EOF while reading length encoded string"))
        }
    }

    fn read_to_null(&mut self) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut chars = self.bytes();
        while let Some(c) = chars.next() {
            let c = try!(c);
            if c == 0u8 {
                break;
            }
            out.push(c);
        }
        Ok(out)
    }

    fn read_bin_value(&mut self, col_type: consts::ColumnType, unsigned: bool) -> io::Result<Value> {
        match col_type {
            ColumnType::MYSQL_TYPE_STRING |
            ColumnType::MYSQL_TYPE_VAR_STRING |
            ColumnType::MYSQL_TYPE_BLOB |
            ColumnType::MYSQL_TYPE_TINY_BLOB |
            ColumnType::MYSQL_TYPE_MEDIUM_BLOB |
            ColumnType::MYSQL_TYPE_LONG_BLOB |
            ColumnType::MYSQL_TYPE_SET |
            ColumnType::MYSQL_TYPE_ENUM |
            ColumnType::MYSQL_TYPE_DECIMAL |
            ColumnType::MYSQL_TYPE_VARCHAR |
            ColumnType::MYSQL_TYPE_BIT |
            ColumnType::MYSQL_TYPE_NEWDECIMAL |
            ColumnType::MYSQL_TYPE_GEOMETRY |
            ColumnType::MYSQL_TYPE_JSON => {
                Ok(Bytes(try!(self.read_lenenc_bytes())))
            },
            ColumnType::MYSQL_TYPE_TINY => {
                if unsigned {
                    Ok(Int(try!(self.read_u8()) as i64))
                } else {
                    Ok(Int(try!(self.read_i8()) as i64))
                }
            },
            ColumnType::MYSQL_TYPE_SHORT |
            ColumnType::MYSQL_TYPE_YEAR => {
                if unsigned {
                    Ok(Int(try!(self.read_u16::<LE>()) as i64))
                } else {
                    Ok(Int(try!(self.read_i16::<LE>()) as i64))
                }
            },
            ColumnType::MYSQL_TYPE_LONG |
            ColumnType::MYSQL_TYPE_INT24 => {
                if unsigned {
                    Ok(Int(try!(self.read_u32::<LE>()) as i64))
                } else {
                    Ok(Int(try!(self.read_i32::<LE>()) as i64))
                }
            },
            ColumnType::MYSQL_TYPE_LONGLONG => {
                if unsigned {
                    Ok(UInt(try!(self.read_u64::<LE>())))
                } else {
                    Ok(Int(try!(self.read_i64::<LE>())))
                }
            },
            ColumnType::MYSQL_TYPE_FLOAT => {
                Ok(Float(try!(self.read_f32::<LE>()) as f64))
            },
            ColumnType::MYSQL_TYPE_DOUBLE => {
                Ok(Float(try!(self.read_f64::<LE>())))
            },
            ColumnType::MYSQL_TYPE_TIMESTAMP |
            ColumnType::MYSQL_TYPE_DATE |
            ColumnType::MYSQL_TYPE_DATETIME => {
                let len = try!(self.read_u8());
                let mut year = 0u16;
                let mut month = 0u8;
                let mut day = 0u8;
                let mut hour = 0u8;
                let mut minute = 0u8;
                let mut second = 0u8;
                let mut micro_second = 0u32;
                if len >= 4u8 {
                    year = try!(self.read_u16::<LE>());
                    month = try!(self.read_u8());
                    day = try!(self.read_u8());
                }
                if len >= 7u8 {
                    hour = try!(self.read_u8());
                    minute = try!(self.read_u8());
                    second = try!(self.read_u8());
                }
                if len == 11u8 {
                    micro_second = try!(self.read_u32::<LE>());
                }
                Ok(Date(year, month, day, hour, minute, second, micro_second))
            },
            ColumnType::MYSQL_TYPE_TIME => {
                let len = try!(self.read_u8());
                let mut is_negative = false;
                let mut days = 0u32;
                let mut hours = 0u8;
                let mut minutes = 0u8;
                let mut seconds = 0u8;
                let mut micro_seconds = 0u32;
                if len >= 8u8 {
                    is_negative = try!(self.read_u8()) == 1u8;
                    days = try!(self.read_u32::<LE>());
                    hours = try!(self.read_u8());
                    minutes = try!(self.read_u8());
                    seconds = try!(self.read_u8());
                }
                if len == 12u8 {
                    micro_seconds = try!(self.read_u32::<LE>());
                }
                Ok(Time(is_negative, days, hours, minutes, seconds, micro_seconds))
            },
            _ => Ok(NULL),
        }
    }

    /// Drops mysql packet paylaod. Returns new seq_id.
    fn drop_packet(&mut self, mut seq_id: u8) -> MyResult<u8> {
        use std::io::ErrorKind::Other;
        loop {
            let payload_len = try!(self.read_uint::<LE>(3)) as usize;
            let srv_seq_id = try!(self.read_u8());
            if srv_seq_id != seq_id {
                return Err(DriverError(PacketOutOfSync));
            }
            seq_id = seq_id.wrapping_add(1);
            if payload_len == 0 {
                break;
            } else {
                if try!(self.fill_buf()).len() < payload_len {
                    return Err(io::Error::new(Other, "Unexpected EOF while reading packet").into())
                }
                self.consume(payload_len);
                if payload_len != consts::MAX_PAYLOAD_LEN {
                    break;
                }
            }
        }
        Ok(seq_id)
    }

    /// Reads mysql packet payload returns it with new seq_id value.
    fn read_packet(&mut self, mut seq_id: u8) -> MyResult<(Vec<u8>, u8)> {
        use std::io::ErrorKind::Other;
        let mut output = Vec::new();
        loop {
            let payload_len = try!(self.read_uint::<LE>(3)) as usize;
            let srv_seq_id = try!(self.read_u8());
            if srv_seq_id != seq_id {
                return Err(DriverError(PacketOutOfSync));
            }
            seq_id = seq_id.wrapping_add(1);
            if payload_len == 0 {
                break;
            } else {
                output.reserve(payload_len);
                let mut chunk = self.take(payload_len as u64);
                let count = try!(chunk.read_to_end(&mut output));
                if count != payload_len {
                    return Err(io::Error::new(Other, "Unexpected EOF while reading packet").into())
                }
                if payload_len != consts::MAX_PAYLOAD_LEN {
                    break;
                }
            }
        }
        Ok((output, seq_id))
    }
}

impl<T: ReadBytesExt + io::BufRead> Read for T {}

pub trait Write: WriteBytesExt {
    fn write_le_uint_n(&mut self, x: u64, len: usize) -> io::Result<()> {
        let mut buf = [0u8; 8];
        let mut offset = 0;
        while offset < len {
            buf[offset] = (((0xFF << (offset * 8)) & x) >> (offset * 8)) as u8;
            offset += 1;
        }
        StdWrite::write_all(self, &buf[..len])
    }

    fn write_lenenc_int(&mut self, x: u64) -> io::Result<()> {
        if x < 251 {
            try!(self.write_u8(x as u8));
            Ok(())
        } else if x < 65_536 {
            try!(self.write_u8(0xFC));
            self.write_le_uint_n(x, 2)
        } else if x < 16_777_216 {
            try!(self.write_u8(0xFD));
            self.write_le_uint_n(x, 3)
        } else {
            try!(self.write_u8(0xFE));
            self.write_le_uint_n(x, 8)
        }
    }

    fn write_lenenc_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        try!(self.write_lenenc_int(bytes.len() as u64));
        self.write_all(bytes)
    }

    fn write_packet(&mut self, data: &[u8], mut seq_id: u8, max_allowed_packet: usize) -> MyResult<u8> {
        if data.len() > max_allowed_packet && max_allowed_packet < consts::MAX_PAYLOAD_LEN {
            return Err(DriverError(PacketTooLarge));
        }
        if data.len() == 0 {
            try!(self.write_all(&[0, 0, 0, seq_id]));
            seq_id = seq_id.wrapping_add(1);
        } else {
            let mut last_was_max = false;
            for chunk in data.chunks(consts::MAX_PAYLOAD_LEN) {
                let chunk_len = chunk.len();
                try!(self.write_le_uint_n(chunk_len as u64, 3));
                try!(self.write_u8(seq_id));
                try!(self.write_all(chunk));
                last_was_max = chunk_len == consts::MAX_PAYLOAD_LEN;
                seq_id = seq_id.wrapping_add(1);
            }
            if last_was_max {
                try!(self.write_all(&[0u8, 0u8, 0u8, seq_id]));
                seq_id = seq_id.wrapping_add(1);
            }
        }
        try!(self.flush());
        Ok(seq_id)
    }
}

impl<T: WriteBytesExt> Write for T {}

#[derive(Debug)]
pub enum Stream {
    #[cfg(unix)]
    SocketStream(BufStream<unix::net::UnixStream>),
    #[cfg(windows)]
    SocketStream(BufStream<np::PipeClient>),
    TcpStream(Option<TcpStream>),
}

trait IoPack: io::Read + io::Write + io::BufRead + 'static { }

impl<T: io::Read + io::Write + 'static> IoPack for BufStream<T> { }

impl AsMut<IoPack> for Stream {
    fn as_mut(&mut self) -> &mut IoPack {
        match *self {
            #[cfg(unix)]
            Stream::SocketStream(ref mut stream) => stream,
            #[cfg(windows)]
            Stream::SocketStream(ref mut stream) => stream,
            Stream::TcpStream(Some(ref mut stream)) => stream.as_mut(),
            _ => panic!("Incomplete stream"),
        }
    }
}

impl Stream {
    #[cfg(unix)]
    pub fn connect_socket(socket: &str,
                          read_timeout: Option<Duration>,
                          write_timeout: Option<Duration>) -> MyResult<Stream>
    {
        match unix::net::UnixStream::connect(socket) {
            Ok(stream) => {
                try!(stream.set_read_timeout(read_timeout));
                try!(stream.set_write_timeout(write_timeout));
                Ok(Stream::SocketStream(BufStream::new(stream)))
            },
            Err(e) => {
                let addr = format!("{}", socket);
                let desc = format!("{}", e);
                Err(DriverError(CouldNotConnect(Some((addr, desc, e.kind())))))
            }
        }
    }

    #[cfg(windows)]
    pub fn connect_socket(socket: &str,
                          read_timeout: Option<Duration>,
                          write_timeout: Option<Duration>) -> MyResult<Stream>
    {
        let full_name = format!(r"\\.\pipe\{}", socket);
        match np::PipeClient::connect(full_name.clone()) {
            Ok(mut stream) => {
                stream.set_read_timeout(read_timeout);
                stream.set_write_timeout(write_timeout);
                Ok(Stream::SocketStream(BufStream::new(stream)))
            },
            Err(e) => {
                let desc = format!("{}", e);
                Err(DriverError(CouldNotConnect(Some((full_name, desc, e.kind())))))
            }
        }
    }

    #[cfg(all(not(unix), not(windows)))]
    fn connect_socket(&mut self) -> MyResult<()> {
        unimplemented!("Sockets is not implemented on current platform");
    }

    pub fn connect_tcp(ip_or_hostname: &str,
                       port: u16,
                       read_timeout: Option<Duration>,
                       write_timeout: Option<Duration>,
                       tcp_keepalive_time: Option<u32>,
                       tcp_connect_timeout: Option<Duration>) -> MyResult<Stream>
    {
        match tcp_connect_timeout {
            Some(timeout) => connect_tcp_stream_timeout((ip_or_hostname, port), timeout),
            None => connect_tcp_stream((ip_or_hostname, port)),
        }.and_then(|stream| {
            stream.set_read_timeout(read_timeout)?;
            stream.set_write_timeout(write_timeout)?;
            stream.set_keepalive_ms(tcp_keepalive_time)?;
            Ok(Stream::TcpStream(Some(TcpStream::Insecure(BufStream::new(stream)))))
        }).map_err(|e| {
            if e.kind() == io::ErrorKind::TimedOut {
                DriverError(ConnectTimeout)
            } else {
                let addr = format!("{}:{}", ip_or_hostname, port);
                let desc = format!("{}", e);
                DriverError(CouldNotConnect(Some((addr, desc, e.kind()))))
            }
        })
    }

    pub fn is_insecure(&self) -> bool {
        match self {
            &Stream::TcpStream(Some(TcpStream::Insecure(_))) => true,
            _ => false,
        }
    }
}

#[cfg(all(feature = "ssl", target_os = "macos"))]
impl Stream {
    pub fn make_secure(mut self, verify_peer: bool, ip_or_hostname: &Option<String>, ssl_opts: &SslOpts)
    -> MyResult<Stream>
    {
        use std::path::Path;
        use std::path::PathBuf;

        fn load_client_cert(path: &Path, pass: &str) -> MyResult<Option<SecIdentity>> {
            use security_framework::import_export::Pkcs12ImportOptions;
            let mut import = Pkcs12ImportOptions::new();
            import.passphrase(pass);
            let mut client_file = try!(::std::fs::File::open(path));
            let mut client_data = Vec::new();
            try!(client_file.read_to_end(&mut client_data));
            let mut identityes = try!(import.import(&*client_data));
            Ok(identityes.pop().map(|x| x.identity))
        }

        fn load_extra_certs(files: &[PathBuf]) -> MyResult<Vec<SecCertificate>> {
            let mut extra_certs = Vec::new();
            for path in files {
                let mut cert_file = try!(::std::fs::File::open(path));
                let mut cert_data = Vec::new();
                try!(cert_file.read_to_end(&mut cert_data));
                extra_certs.push(try!(SecCertificate::from_der(&*cert_data)));
            }
            Ok(extra_certs)
        }

        if self.is_insecure() {
            let mut ctx: SslContext = try!(SslContext::new(ProtocolSide::Client, ConnectionType::Stream));
            match *ssl_opts {
                Some(ref ssl_opts) => {
                    if verify_peer {
                        try!(ctx.set_peer_domain_name(ip_or_hostname.as_ref().unwrap_or(&("localhost".into()))));
                    }
                    // Taken from gmail.com
                    try!(ctx.set_enabled_ciphers(&[
                        CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
                        CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
                        CipherSuite::TLS_ECDHE_RSA_WITH_RC4_128_SHA,
                        CipherSuite::TLS_RSA_WITH_AES_128_GCM_SHA256,
                        CipherSuite::TLS_RSA_WITH_AES_128_CBC_SHA256,
                        CipherSuite::TLS_RSA_WITH_AES_128_CBC_SHA,
                        CipherSuite::TLS_RSA_WITH_RC4_128_SHA,
                        CipherSuite::TLS_RSA_WITH_RC4_128_MD5,
                        CipherSuite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
                        CipherSuite::TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384,
                        CipherSuite::TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
                        CipherSuite::TLS_RSA_WITH_AES_256_GCM_SHA384,
                        CipherSuite::TLS_RSA_WITH_AES_256_CBC_SHA256,
                        CipherSuite::TLS_RSA_WITH_AES_256_CBC_SHA,
                        CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
                        CipherSuite::TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA,
                        CipherSuite::TLS_RSA_WITH_3DES_EDE_CBC_SHA,
                    ]));

                    if let Some((ref path, ref pass, ref certs)) = *ssl_opts {
                        if let Some(identity) = try!(load_client_cert(path, pass)) {
                            let extra_certs = try!(load_extra_certs(certs));
                            try!(ctx.set_certificate(&identity, &*extra_certs));
                        }
                    }

                    match self {
                        Stream::TcpStream(ref mut opt_stream) if opt_stream.is_some() => {
                            let stream = opt_stream.take().unwrap();
                            match stream {
                                TcpStream::Insecure(mut stream) => {
                                    try!(stream.flush());
                                    let sstream = match ctx.handshake(stream.into_inner().unwrap()) {
                                        Ok(sstream) => sstream,
                                        Err(HandshakeError::Failure(err)) => {
                                            return Err(err.into())
                                        },
                                        Err(HandshakeError::Interrupted(_)) => unreachable!(),
                                    };
                                    Ok(Stream::TcpStream(Some(TcpStream::Secure(BufStream::new(sstream)))))
                                },
                                _ => unreachable!(),
                            }

                        },
                        _ => unreachable!(),
                    }
                },
                _ => unreachable!(),
            }
        } else {
            Ok(self)
        }
    }
}

#[cfg(all(feature = "ssl", not(target_os = "macos"), unix))]
impl Stream {
    pub fn make_secure(mut self, verify_peer: bool, _: &Option<String>, ssl_opts: &SslOpts) -> MyResult<Stream>
    {
        if self.is_insecure() {
            let mut ctx = try!(SslContext::builder(ssl::SslMethod::tls()));
            let mode = if verify_peer {
                ssl::SSL_VERIFY_PEER
            } else {
                ssl::SSL_VERIFY_NONE
            };
            ctx.set_verify(mode);
            match *ssl_opts {
                Some((ref ca_cert, None)) => try!(ctx.set_ca_file(&ca_cert)),
                Some((ref ca_cert, Some((ref client_cert, ref client_key)))) => {
                    try!(ctx.set_ca_file(&ca_cert));
                    try!(ctx.set_certificate_file(&client_cert, x509::X509_FILETYPE_PEM));
                    try!(ctx.set_private_key_file(&client_key, x509::X509_FILETYPE_PEM));
                },
                _ => unreachable!(),
            }
            match self {
                Stream::TcpStream(ref mut opt_stream) if opt_stream.is_some() => {
                    let stream = opt_stream.take().unwrap();
                    match stream {
                        TcpStream::Insecure(stream) => {
                            let ctx = ctx.build();
                            let sstream = match try!(ssl::Ssl::new(&ctx)).connect(stream.into_inner().unwrap()) {
                                Ok(sstream) => sstream,
                                Err(handshake_err) => match handshake_err {
                                    ssl::HandshakeError::SetupFailure(err) => return Err(err.into()),
                                    ssl::HandshakeError::Failure(mid_stream) => return Err(mid_stream.into_error().into()),
                                    ssl::HandshakeError::Interrupted(_) => unreachable!("Interrupted"),
                                },
                            };
                            Ok(Stream::TcpStream(Some(TcpStream::Secure(BufStream::new(sstream)))))
                        },
                        _ => unreachable!(),
                    }

                },
                _ => unreachable!(),
            }
        } else {
            Ok(self)
        }
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        if let &mut Stream::TcpStream(None) = self {
            return;
        }
        let _ = self.as_mut().write_packet(&[Command::COM_QUIT as u8], 0, consts::MAX_PAYLOAD_LEN);
        let _ = self.as_mut().flush();
    }
}

pub enum TcpStream {
    #[cfg(all(feature = "ssl", any(unix, target_os = "macos")))]
    Secure(BufStream<SslStream<net::TcpStream>>),
    Insecure(BufStream<net::TcpStream>),
}

impl AsMut<IoPack> for TcpStream {
    fn as_mut(&mut self) -> &mut IoPack {
        match *self {
            #[cfg(all(feature = "ssl", any(unix, target_os = "macos")))]
            TcpStream::Secure(ref mut stream) => stream,
            TcpStream::Insecure(ref mut stream) => stream,
        }
    }
}

impl fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            #[cfg(all(feature = "ssl", any(unix, target_os = "macos")))]
            TcpStream::Secure(_) => write!(f, "Secure stream"),
            TcpStream::Insecure(_) => write!(f, "Insecure stream"),
        }
    }
}
