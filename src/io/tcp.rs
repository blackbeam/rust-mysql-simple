use libc::*;
use net2::{TcpBuilder, TcpStreamExt};
use nix::errno::Errno;
use nix::sys::select;
use nix::sys::socket;
use nix::sys::time::{TimeVal, TimeValLike};
use std::io;
use std::net::{TcpStream, SocketAddr, ToSocketAddrs};
#[cfg(unix)]
use std::os::unix::prelude::*;
#[cfg(target_os = "windows")]
use std::os::windows::prelude;
use std::time::Duration;

pub struct MyTcpBuilder<T> {
    address: T,
    bind_address: Option<SocketAddr>,
    connect_timeout: Option<Duration>,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    keepalive_time_ms: Option<u32>,
}

impl<T: ToSocketAddrs> MyTcpBuilder<T> {
    pub fn keepalive_time_ms(&mut self, keepalive_time_ms: Option<u32>) -> &mut Self {
        self.keepalive_time_ms = keepalive_time_ms;
        self
    }

    pub fn write_timeout(&mut self, write_timeout: Option<Duration>) -> &mut Self {
        self.write_timeout = write_timeout;
        self
    }

    pub fn read_timeout(&mut self, read_timeout: Option<Duration>) -> &mut Self {
        self.read_timeout = read_timeout;
        self
    }

    pub fn bind_address<U>(&mut self, bind_address: Option<U>) -> &mut Self
        where U: Into<SocketAddr>
    {
        self.bind_address = bind_address.map(Into::into);
        self
    }

    pub fn connect_timeout(&mut self, timeout: Option<Duration>) -> &mut Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn new(address: T) -> MyTcpBuilder<T> {
        MyTcpBuilder {
            address,
            bind_address: None,
            connect_timeout: None,
            read_timeout: None,
            write_timeout: None,
            keepalive_time_ms: None,
        }
    }

    pub fn connect(self) -> io::Result<TcpStream> {
        let MyTcpBuilder {
            address,
            bind_address,
            connect_timeout,
            read_timeout,
            write_timeout,
            keepalive_time_ms,
        } = self;
        let err_msg = if bind_address.is_none() {
            "could not connect to any address"
        } else {
            "could not connect to any address with specified bind address"
        };
        let err = io::Error::new(io::ErrorKind::Other, err_msg);
        address.to_socket_addrs()?
            .fold(Err(err), |prev, sock_addr| {
                prev.or_else(|_| {
                    let builder = if sock_addr.is_ipv4() {
                        TcpBuilder::new_v4()?
                    } else {
                        TcpBuilder::new_v6()?
                    };
                    if let Some(bind_address) = bind_address {
                        if bind_address.is_ipv4() == sock_addr.is_ipv4() {
                            builder.bind(bind_address)?;
                        }
                    }
                    if let Some(connect_timeout) = connect_timeout {
                        connect_fd_timeout(builder.as_raw_fd(), &sock_addr, connect_timeout)?;
                        builder.to_tcp_stream()
                    } else {
                        builder.connect(sock_addr)
                    }
                })
            })
            .and_then(|stream| {
                stream.set_read_timeout(read_timeout)?;
                stream.set_write_timeout(write_timeout)?;
                stream.set_keepalive_ms(keepalive_time_ms)?;
                Ok(stream)
            })
    }
}

#[cfg(unix)]
fn set_non_blocking(fd: RawFd, non_blocking: bool) -> io::Result<()> {
    let mut non_blocking = non_blocking as c_ulong;
    let result = unsafe {
        ioctl(fd, FIONBIO, &mut non_blocking)
    };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(unix))]
fn set_non_blocking(socket: RawSocket, non_blocking: bool) -> io::Result<()> {
    unimplemented!();
}

#[cfg(unix)]
fn connect_fd_timeout(fd: RawFd, sock_addr: &SocketAddr, timeout: Duration) -> io::Result<()> {
    set_non_blocking(fd, true)?;

    let inet_addr = socket::InetAddr::from_std(sock_addr);
    let sock_addr = socket::SockAddr::Inet(inet_addr);
    match socket::connect(fd, &sock_addr) {
        Ok(_) => (),
        Err(err) => {
            match err.errno() {
                Errno::EALREADY |
                Errno::EINPROGRESS => (),
                errno => {
                    return Err(io::Error::from_raw_os_error(errno as i32));
                },
            }
        }
    }

    let mut fd_set = select::FdSet::new();
    let socket_fd = fd;
    fd_set.insert(socket_fd);
    let mut timeout_timeval = TimeVal::microseconds(timeout.as_secs() as i64 * 1000 * 1000);

    let select_res = select::select(
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

    let socket_error_code = socket::getsockopt(
        socket_fd,
        socket::sockopt::SocketError,
    )?;

    if socket_error_code != 0 {
        return Err(io::Error::from_raw_os_error(socket_error_code));
    }

    set_non_blocking(fd, false)
}

#[cfg(not(unix))]
fn connect_fd_timeout(fd: RawFd, sock_addr: &SocketAddr, timeout: Duration) -> io::Result<()> {
    panic!("tcp_connect_timeout is unix-only feature");
}
