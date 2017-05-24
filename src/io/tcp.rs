use net2::{TcpBuilder, TcpStreamExt};
#[cfg(unix)]
use nix::errno::Errno;
#[cfg(unix)]
use nix::sys::select;
#[cfg(unix)]
use nix::sys::socket;
#[cfg(unix)]
use nix::sys::time::{TimeVal, TimeValLike};
use std::io;
use std::mem;
use std::net::{TcpStream, SocketAddr, ToSocketAddrs};
#[cfg(target_os = "windows")]
use std::os::raw::*;
#[cfg(unix)]
use std::os::unix::prelude::*;
#[cfg(target_os = "windows")]
use std::os::windows::prelude::*;
#[cfg(target_os = "windows")]
use std::ptr;
use std::time::Duration;
#[cfg(target_os = "windows")]
use winapi::*;
#[cfg(target_os = "windows")]
use ws2_32::*;

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
            address: address,
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

#[cfg(target_os = "windows")]
fn connect_fd_timeout(socket: RawSocket,
                      sock_addr: &SocketAddr,
                      timeout: Duration) -> io::Result<()> {
    set_non_blocking(socket, true)?;
    let (name, name_len) = match *sock_addr {
        SocketAddr::V4(ref a) => {
            (a as *const _ as *const _, mem::size_of_val(a) as c_int)
        },
        SocketAddr::V6(ref a) => {
            (a as *const _ as *const _, mem::size_of_val(a) as c_int)
        }
    };
    let result = unsafe { connect(socket, name, name_len) };
    if result == SOCKET_ERROR {
        let err = io::Error::last_os_error();
        match err.raw_os_error().map(|x| x as u32) {
            Some(WSAEWOULDBLOCK) => {
                let mut write_fds = fd_set {
                    fd_count: 1,
                    fd_array: [0; FD_SETSIZE]
                };
                write_fds.fd_array[0] = socket;
                let mut err_fds = write_fds.clone();
                let timeout = timeval {
                    tv_sec: timeout.as_secs() as c_long,
                    tv_usec: 0,
                };

                let result = unsafe {
                    select(0, ptr::null_mut(), &mut write_fds, &mut err_fds, &timeout)
                };

                if result == 0 {
                    return Err(io::ErrorKind::TimedOut.into());
                } else if result == SOCKET_ERROR {
                    return Err(io::Error::last_os_error());
                } else {
                    let mut error = None;
                    for i in 0..(err_fds.fd_count as usize) {
                        if err_fds.fd_array[i] == socket {
                            error = Some(true);
                        }
                    }
                    for i in 0..(write_fds.fd_count as usize) {
                        if write_fds.fd_array[i] == socket {
                            error = Some(false);
                        }
                    }
                    match error {
                        Some(false) => (),
                        Some(true) => {
                            let mut opt_val = 0i32;
                            let mut opt_len = mem::size_of::<i32>() as c_int;
                            let result = unsafe {
                                getsockopt(socket,
                                           SOL_SOCKET,
                                           SO_ERROR,
                                           &mut opt_val as *mut _ as *mut _,
                                           &mut opt_len)
                            };
                            return Err(io::Error::from_raw_os_error(result));
                        },
                        None => unreachable!(),
                    }
                }
            },
            _ => return Err(err),
        }
    }
    set_non_blocking(socket, false)
}
