use bufstream::BufStream;
use byteorder::{LittleEndian as LE, ReadBytesExt, WriteBytesExt};
use io_enum::*;
#[cfg(windows)]
use named_pipe as np;
use native_tls::{Certificate, Identity, TlsConnector, TlsStream};

use std::fmt;
use std::fs::File;
use std::io::{self, Read as _};
use std::net::{self, SocketAddr};
#[cfg(unix)]
use std::os::unix;
use std::time::Duration;

use crate::error::DriverError::{ConnectTimeout, CouldNotConnect};
use crate::error::Error::DriverError;
use crate::error::Result as MyResult;
use crate::SslOpts;

mod tcp;

pub trait Read: ReadBytesExt + io::BufRead {
    fn read_lenenc_int(&mut self) -> io::Result<u64> {
        let head_byte = self.read_u8()?;
        let length = match head_byte {
            0xfc => 2,
            0xfd => 3,
            0xfe => 8,
            x => return Ok(u64::from(x)),
        };
        let out = self.read_uint::<LE>(length)?;
        Ok(out)
    }

    fn read_lenenc_bytes(&mut self) -> io::Result<Vec<u8>> {
        let len = self.read_lenenc_int()?;
        let mut out = Vec::with_capacity(len as usize);
        let count = if len > 0 {
            self.take(len).read_to_end(&mut out)?
        } else {
            0
        };
        if count as u64 == len {
            Ok(out)
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Unexpected EOF while reading length encoded string",
            ))
        }
    }

    fn read_to_null(&mut self) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        for c in self.bytes() {
            let c = c?;
            if c == 0 {
                break;
            }
            out.push(c);
        }
        Ok(out)
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
        self.write_all(&buf[..len])
    }

    fn write_lenenc_int(&mut self, x: u64) -> io::Result<()> {
        if x < 251 {
            self.write_u8(x as u8)?;
            Ok(())
        } else if x < 65_536 {
            self.write_u8(0xFC)?;
            self.write_le_uint_n(x, 2)
        } else if x < 16_777_216 {
            self.write_u8(0xFD)?;
            self.write_le_uint_n(x, 3)
        } else {
            self.write_u8(0xFE)?;
            self.write_le_uint_n(x, 8)
        }
    }

    fn write_lenenc_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.write_lenenc_int(bytes.len() as u64)?;
        self.write_all(bytes)
    }
}

impl<T: WriteBytesExt> Write for T {}

#[derive(Debug, Read, Write)]
pub enum Stream {
    #[cfg(unix)]
    SocketStream(BufStream<unix::net::UnixStream>),
    #[cfg(windows)]
    SocketStream(BufStream<np::PipeClient>),
    TcpStream(TcpStream),
}

impl Stream {
    #[cfg(unix)]
    pub fn connect_socket(
        socket: &str,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) -> MyResult<Stream> {
        match unix::net::UnixStream::connect(socket) {
            Ok(stream) => {
                stream.set_read_timeout(read_timeout)?;
                stream.set_write_timeout(write_timeout)?;
                Ok(Stream::SocketStream(BufStream::new(stream)))
            }
            Err(e) => {
                let addr = socket.to_string();
                let desc = e.to_string();
                Err(DriverError(CouldNotConnect(Some((addr, desc, e.kind())))))
            }
        }
    }

    #[cfg(windows)]
    pub fn connect_socket(
        socket: &str,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) -> MyResult<Stream> {
        let full_name = format!(r"\\.\pipe\{}", socket);
        match np::PipeClient::connect(full_name.clone()) {
            Ok(mut stream) => {
                stream.set_read_timeout(read_timeout);
                stream.set_write_timeout(write_timeout);
                Ok(Stream::SocketStream(BufStream::new(stream)))
            }
            Err(e) => {
                let desc = format!("{}", e);
                Err(DriverError(CouldNotConnect(Some((
                    full_name,
                    desc,
                    e.kind(),
                )))))
            }
        }
    }

    #[cfg(all(not(unix), not(windows)))]
    fn connect_socket(&mut self) -> MyResult<()> {
        unimplemented!("Sockets is not implemented on current platform");
    }

    pub fn connect_tcp(
        ip_or_hostname: &str,
        port: u16,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
        tcp_keepalive_time: Option<u32>,
        nodelay: bool,
        tcp_connect_timeout: Option<Duration>,
        bind_address: Option<SocketAddr>,
    ) -> MyResult<Stream> {
        let mut builder = tcp::MyTcpBuilder::new((ip_or_hostname, port));
        builder
            .connect_timeout(tcp_connect_timeout)
            .read_timeout(read_timeout)
            .write_timeout(write_timeout)
            .keepalive_time_ms(tcp_keepalive_time)
            .nodelay(nodelay)
            .bind_address(bind_address);
        builder
            .connect()
            .map(|stream| Stream::TcpStream(TcpStream::Insecure(BufStream::new(stream))))
            .map_err(|err| {
                if err.kind() == io::ErrorKind::TimedOut {
                    DriverError(ConnectTimeout)
                } else {
                    let addr = format!("{}:{}", ip_or_hostname, port);
                    let desc = format!("{}", err);
                    DriverError(CouldNotConnect(Some((addr, desc, err.kind()))))
                }
            })
    }

    pub fn is_insecure(&self) -> bool {
        match self {
            Stream::TcpStream(TcpStream::Insecure(_)) => true,
            _ => false,
        }
    }

    pub fn is_socket(&self) -> bool {
        match self {
            Stream::SocketStream(_) => true,
            _ => false,
        }
    }

    pub fn make_secure(self, ip_or_hostname: Option<&str>, ssl_opts: SslOpts) -> MyResult<Stream> {
        if self.is_socket() {
            // won't secure socket connection
            return Ok(self);
        }

        let domain = ip_or_hostname.unwrap_or("127.0.0.1");
        let mut builder = TlsConnector::builder();
        match ssl_opts.root_cert_path() {
            Some(root_cert_path) => {
                let mut root_cert_der = vec![];
                let mut root_cert_file = File::open(root_cert_path)?;
                root_cert_file.read_to_end(&mut root_cert_der)?;
                let root_cert = Certificate::from_der(&*root_cert_der)?;
                builder.add_root_certificate(root_cert);
            }
            None => (),
        }
        if let Some(pkcs12_path) = ssl_opts.pkcs12_path() {
            let der = std::fs::read(pkcs12_path)?;
            let identity = Identity::from_pkcs12(&*der, ssl_opts.password().unwrap_or(""))?;
            builder.identity(identity);
        }
        builder.danger_accept_invalid_hostnames(ssl_opts.skip_domain_validation());
        builder.danger_accept_invalid_certs(ssl_opts.accept_invalid_certs());
        let tls_connector = builder.build()?;
        match self {
            Stream::TcpStream(tcp_stream) => match tcp_stream {
                TcpStream::Insecure(insecure_stream) => {
                    let inner = insecure_stream.into_inner().map_err(io::Error::from)?;
                    let secure_stream = tls_connector.connect(domain, inner)?;
                    Ok(Stream::TcpStream(TcpStream::Secure(BufStream::new(
                        secure_stream,
                    ))))
                }
                TcpStream::Secure(_) => Ok(Stream::TcpStream(tcp_stream)),
            },
            _ => unreachable!(),
        }
    }
}

#[derive(Read, Write)]
pub enum TcpStream {
    Secure(BufStream<TlsStream<net::TcpStream>>),
    Insecure(BufStream<net::TcpStream>),
}

impl fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            TcpStream::Secure(ref s) => write!(f, "Secure stream {:?}", s),
            TcpStream::Insecure(ref s) => write!(f, "Insecure stream {:?}", s),
        }
    }
}
