// Copyright (c) 2020 rust-mysql-simple contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use bufstream::BufStream;
use io_enum::*;
#[cfg(windows)]
use named_pipe as np;

#[cfg(unix)]
use std::os::{
    unix,
    unix::io::{AsRawFd, RawFd},
};
use std::{
    fmt, io,
    net::{self, SocketAddr},
    time::Duration,
};

use crate::error::{
    DriverError::{ConnectTimeout, CouldNotConnect},
    Error::DriverError,
    Result as MyResult,
};

mod tcp;
mod tls;

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
        #[cfg(any(target_os = "linux", target_os = "macos",))]
        tcp_keepalive_probe_interval_secs: Option<u32>,
        #[cfg(any(target_os = "linux", target_os = "macos",))] tcp_keepalive_probe_count: Option<
            u32,
        >,
        #[cfg(target_os = "linux")] tcp_user_timeout: Option<u32>,
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
        #[cfg(any(target_os = "linux", target_os = "macos",))]
        builder.keepalive_probe_interval_secs(tcp_keepalive_probe_interval_secs);
        #[cfg(any(target_os = "linux", target_os = "macos",))]
        builder.keepalive_probe_count(tcp_keepalive_probe_count);
        #[cfg(target_os = "linux")]
        builder.user_timeout(tcp_user_timeout);
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

    #[cfg(all(not(feature = "native-tls"), not(feature = "rustls")))]
    pub fn make_secure(self, _host: url::Host, _ssl_opts: crate::SslOpts) -> MyResult<Stream> {
        panic!(
            "Client had asked for TLS connection but TLS support is disabled. \
            Please enable one of the following features: [\"native-tls\", \"rustls\"]"
        )
    }
}

#[cfg(unix)]
impl AsRawFd for Stream {
    fn as_raw_fd(&self) -> RawFd {
        match self {
            Stream::SocketStream(stream) => stream.get_ref().as_raw_fd(),
            Stream::TcpStream(stream) => stream.as_raw_fd(),
        }
    }
}

#[derive(Read, Write)]
pub enum TcpStream {
    #[cfg(feature = "native-tls")]
    Secure(BufStream<native_tls::TlsStream<net::TcpStream>>),
    #[cfg(feature = "rustls")]
    Secure(BufStream<rustls::StreamOwned<rustls::ClientConnection, net::TcpStream>>),
    Insecure(BufStream<net::TcpStream>),
}

#[cfg(unix)]
impl AsRawFd for TcpStream {
    fn as_raw_fd(&self) -> RawFd {
        match self {
            #[cfg(feature = "native-tls")]
            TcpStream::Secure(stream) => stream.get_ref().get_ref().as_raw_fd(),
            #[cfg(feature = "rustls")]
            TcpStream::Secure(stream) => stream.get_ref().get_ref().as_raw_fd(),
            TcpStream::Insecure(stream) => stream.get_ref().as_raw_fd(),
        }
    }
}

impl fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            #[cfg(feature = "native-tls")]
            TcpStream::Secure(ref s) => write!(f, "Secure stream {:?}", s),
            #[cfg(feature = "rustls")]
            TcpStream::Secure(ref s) => write!(f, "Secure stream {:?}", s),
            TcpStream::Insecure(ref s) => write!(f, "Insecure stream {:?}", s),
        }
    }
}
