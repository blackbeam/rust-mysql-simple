use std::sync::{Arc, Mutex};
use std::{fmt, io};

use crate::Conn;

pub(crate) type LocalInfileInner =
    Arc<Mutex<dyn for<'a> FnMut(&'a [u8], &'a mut LocalInfile<'_>) -> io::Result<()> + Send>>;

/// Callback to handle requests for local files.
/// Consult [Mysql documentation](https://dev.mysql.com/doc/refman/5.7/en/load-data.html) for the
/// format of local infile data.
///
/// # Support
///
/// Note that older versions of Mysql server may not support this functionality.
///
/// ```rust
/// # use std::io::Write;
/// # use mysql::{
/// #     Pool,
/// #     Opts,
/// #     OptsBuilder,
/// #     LocalInfileHandler,
/// #     from_row,
/// #     error::Error
/// # };
/// # fn get_opts() -> Opts {
/// #     let user = "root";
/// #     let addr = "127.0.0.1";
/// #     let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or("password".to_string());
/// #     let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
/// #                                .map(|my_port| my_port.parse().ok().unwrap_or(3307))
/// #                                .unwrap_or(3307);
/// #     let mut builder = OptsBuilder::default();
/// #     builder.user(Some(user))
/// #            .pass(Some(pwd))
/// #            .ip_or_hostname(Some(addr))
/// #            .tcp_port(port)
/// #            .db_name("mysql".into())
/// #            .init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'"]);
/// #     builder.into()
/// # }
/// # let opts = get_opts();
/// # let pool = Pool::new_manual(1, 1, opts).unwrap();
/// # pool.prep_exec("CREATE TEMPORARY TABLE mysql.Users (id INT, name TEXT, age INT, email TEXT)", ()).unwrap();
/// # pool.prep_exec("INSERT INTO mysql.Users (id, name, age, email) VALUES (?, ?, ?, ?)",
/// #                (1, "John", 17, "foo@bar.baz")).unwrap();
/// # let mut conn = pool.get_conn().unwrap();
/// conn.query("CREATE TEMPORARY TABLE mysql.tbl(a TEXT)").unwrap();
///
/// conn.set_local_infile_handler(Some(
///     LocalInfileHandler::new(|file_name, writer| {
///         writer.write_all(b"row1: file name is ")?;
///         writer.write_all(file_name)?;
///         writer.write_all(b"\n")?;
///
///         writer.write_all(b"row2: foobar\n")
///     })
/// ));
///
/// match conn.query("LOAD DATA LOCAL INFILE 'file_name' INTO TABLE mysql.tbl") {
///     Ok(_) => (),
///     Err(Error::MySqlError(ref e)) if e.code == 1148 => {
///         // functionality is not supported by the server
///         return;
///     },
///     err => {
///         err.unwrap();
///     },
/// }
///
/// let mut row_num = 0;
/// for (row_idx, row) in conn.query("SELECT * FROM mysql.tbl").unwrap().enumerate() {
///     row_num = row_idx + 1;
///     let row: (String,) = from_row(row.unwrap());
///     match row_num {
///         1 => assert_eq!(row.0, "row1: file name is file_name"),
///         2 => assert_eq!(row.0, "row2: foobar"),
///         _ => unreachable!(),
///     }
/// }
///
/// assert_eq!(row_num, 2);
/// ```
#[derive(Clone)]
pub struct LocalInfileHandler(pub(crate) LocalInfileInner);

impl LocalInfileHandler {
    pub fn new<F>(f: F) -> Self
    where
        F: for<'a> FnMut(&'a [u8], &'a mut LocalInfile<'_>) -> io::Result<()> + Send + 'static,
    {
        LocalInfileHandler(Arc::new(Mutex::new(f)))
    }
}

impl PartialEq for LocalInfileHandler {
    fn eq(&self, other: &LocalInfileHandler) -> bool {
        (&*self.0 as *const _) == (&*other.0 as *const _)
    }
}

impl Eq for LocalInfileHandler {}

impl fmt::Debug for LocalInfileHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "LocalInfileHandler(...)")
    }
}

/// Local in-file stream.
/// The callback will be passed a reference to this stream, which it
/// should use to write the contents of the requested file.
/// See [LocalInfileHandler](struct.LocalInfileHandler.html) documentation for example.
#[derive(Debug)]
pub struct LocalInfile<'a> {
    buffer: io::Cursor<Box<[u8]>>,
    conn: &'a mut Conn,
}

impl<'a> LocalInfile<'a> {
    pub(crate) fn new(buffer: io::Cursor<Box<[u8]>>, conn: &'a mut Conn) -> Self {
        Self { buffer, conn }
    }
}

impl<'a> io::Write for LocalInfile<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let result = self.buffer.write(buf);
        if result.is_ok() && self.buffer.position() as usize >= self.buffer.get_ref().len() {
            self.flush()?;
        }
        result
    }

    fn flush(&mut self) -> io::Result<()> {
        let n = self.buffer.position() as usize;
        if n > 0 {
            let range = &self.buffer.get_ref()[..n];
            self.conn
                .write_packet(range)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, Box::new(e)))?;
        }
        self.buffer.set_position(0);
        Ok(())
    }
}
