use net2::{TcpBuilder, TcpStreamExt};
#[cfg(unix)]
use nix::{
    errno::Errno,
    poll::{self, PollFlags},
    sys::socket,
};
use std::io;
use std::mem;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
#[cfg(unix)]
use std::os::unix::prelude::*;
use std::time::Duration;
#[cfg(target_os = "windows")]
use std::{os::raw::*, os::windows::prelude::*, ptr};
#[cfg(target_os = "windows")]
use winapi::um::winsock2::*;

pub struct MyTcpBuilder<T> {
    address: T,
    bind_address: Option<SocketAddr>,
    connect_timeout: Option<Duration>,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    keepalive_time_ms: Option<u32>,
    nodelay: bool,
}

impl<T: ToSocketAddrs> MyTcpBuilder<T> {
    pub fn keepalive_time_ms(&mut self, keepalive_time_ms: Option<u32>) -> &mut Self {
        self.keepalive_time_ms = keepalive_time_ms;
        self
    }

    pub fn nodelay(&mut self, nodelay: bool) -> &mut Self {
        self.nodelay = nodelay;
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
    where
        U: Into<SocketAddr>,
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
            nodelay: true,
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
            nodelay,
        } = self;
        let err_msg = if bind_address.is_none() {
            "could not connect to any address"
        } else {
            "could not connect to any address with specified bind address"
        };
        let err = io::Error::new(io::ErrorKind::Other, err_msg);
        address
            .to_socket_addrs()?
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
                        #[cfg(unix)]
                        connect_fd_timeout(builder.as_raw_fd(), &sock_addr, connect_timeout)?;
                        #[cfg(target_os = "windows")]
                        connect_fd_timeout(builder.as_raw_socket(), &sock_addr, connect_timeout)?;
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
                stream.set_nodelay(nodelay)?;
                Ok(stream)
            })
    }
}

#[cfg(unix)]
fn set_non_blocking(fd: RawFd, non_blocking: bool) -> io::Result<()> {
    let stream = unsafe { TcpStream::from_raw_fd(fd) };
    let result = stream.set_nonblocking(non_blocking);
    mem::forget(stream);
    result
}

#[cfg(target_os = "windows")]
fn set_non_blocking(socket: RawSocket, non_blocking: bool) -> io::Result<()> {
    let stream = unsafe { TcpStream::from_raw_socket(socket) };
    let result = stream.set_nonblocking(non_blocking);
    mem::forget(stream);
    result
}

#[cfg(unix)]
fn connect_fd_timeout(fd: RawFd, sock_addr: &SocketAddr, timeout: Duration) -> io::Result<()> {
    set_non_blocking(fd, true)?;

    let inet_addr = socket::InetAddr::from_std(sock_addr);
    let sock_addr = socket::SockAddr::Inet(inet_addr);
    match socket::connect(fd, &sock_addr) {
        Ok(_) => (),
        Err(err) => match err {
            ::nix::Error::Sys(Errno::EALREADY) | ::nix::Error::Sys(Errno::EINPROGRESS) => (),
            ::nix::Error::Sys(errno) => return Err(io::Error::from_raw_os_error(errno as i32)),
            _ => return Err(io::Error::new(io::ErrorKind::Other, err)),
        },
    }

    let socket_fd = fd;
    let mut poll_fds = [poll::PollFd::new(socket_fd, PollFlags::POLLIN)];
    let timeout_millis = timeout.as_secs() as i32 * 1000;

    let poll_res = poll::poll(&mut poll_fds, timeout_millis);

    let poll_res = poll_res.map_err(|err| match err {
        ::nix::Error::Sys(errno) => io::Error::from_raw_os_error(errno as i32),
        _ => io::Error::new(io::ErrorKind::Other, err),
    })?;

    if poll_res == -1 {
        return Err(io::Error::last_os_error());
    }

    if poll_res != 1 {
        return Err(io::ErrorKind::TimedOut.into());
    }

    let socket_error_code = socket::getsockopt(socket_fd, socket::sockopt::SocketError);

    let socket_error_code = socket_error_code.map_err(|err| match err {
        ::nix::Error::Sys(errno) => io::Error::from_raw_os_error(errno as i32),
        _ => io::Error::new(io::ErrorKind::Other, err),
    })?;

    if socket_error_code != 0 {
        return Err(io::Error::from_raw_os_error(socket_error_code));
    }

    set_non_blocking(fd, false)
}

#[cfg(target_os = "windows")]
fn connect_fd_timeout(
    socket: RawSocket,
    sock_addr: &SocketAddr,
    timeout: Duration,
) -> io::Result<()> {
    set_non_blocking(socket, true)?;
    let (name, name_len) = match *sock_addr {
        SocketAddr::V4(ref a) => (a as *const _ as *const _, mem::size_of_val(a) as c_int),
        SocketAddr::V6(ref a) => (a as *const _ as *const _, mem::size_of_val(a) as c_int),
    };
    let result = unsafe { connect(socket as usize, name, name_len) };
    if result == SOCKET_ERROR {
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(WSAEWOULDBLOCK) => {
                let mut write_fds = fd_set {
                    fd_count: 1,
                    fd_array: [0; FD_SETSIZE],
                };
                write_fds.fd_array[0] = socket as usize;
                let mut err_fds = write_fds.clone();
                let timeout = timeval {
                    tv_sec: timeout.as_secs() as c_long,
                    tv_usec: 0,
                };

                let result =
                    unsafe { select(0, ptr::null_mut(), &mut write_fds, &mut err_fds, &timeout) };

                if result == 0 {
                    return Err(io::ErrorKind::TimedOut.into());
                } else if result == SOCKET_ERROR {
                    return Err(io::Error::last_os_error());
                } else {
                    let mut error = None;
                    for i in 0..(err_fds.fd_count as usize) {
                        if err_fds.fd_array[i] == socket as usize {
                            error = Some(true);
                        }
                    }
                    for i in 0..(write_fds.fd_count as usize) {
                        if write_fds.fd_array[i] == socket as usize {
                            error = Some(false);
                        }
                    }
                    match error {
                        Some(false) => (),
                        Some(true) => {
                            let mut opt_val = 0i32;
                            let mut opt_len = mem::size_of::<i32>() as c_int;
                            let result = unsafe {
                                getsockopt(
                                    socket as usize,
                                    SOL_SOCKET,
                                    SO_ERROR,
                                    &mut opt_val as *mut _ as *mut _,
                                    &mut opt_len,
                                )
                            };
                            return Err(io::Error::from_raw_os_error(result));
                        }
                        None => unreachable!(),
                    }
                }
            }
            _ => return Err(err),
        }
    }
    set_non_blocking(socket, false)
}
