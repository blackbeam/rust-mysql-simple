use std::borrow::Borrow;
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration as StdDuration;

use crate::time::{Duration, SteadyTime};

use super::super::error::Result as MyResult;
use super::super::error::{DriverError, Error};
use super::GenericConnection;
use super::IsolationLevel;
use super::LocalInfileHandler;
use super::Transaction;
use super::{Conn, Opts, QueryResult, Stmt};
use crate::myc::named_params::parse_named_params;
use crate::myc::row::convert::{from_row, FromRow};
use crate::Params;
use crate::Row;

#[derive(Debug)]
struct InnerPool {
    opts: Opts,
    pool: VecDeque<Conn>,
}

impl InnerPool {
    fn new(min: usize, max: usize, opts: Opts) -> MyResult<InnerPool> {
        if min > max || max == 0 {
            return Err(Error::DriverError(DriverError::InvalidPoolConstraints));
        }
        let mut pool = InnerPool {
            opts: opts,
            pool: VecDeque::with_capacity(max),
        };
        for _ in 0..min {
            pool.new_conn()?;
        }
        Ok(pool)
    }
    fn new_conn(&mut self) -> MyResult<()> {
        match Conn::new(self.opts.clone()) {
            Ok(conn) => {
                self.pool.push_back(conn);
                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}

/// `Pool` serves to provide you with a [`PooledConn`](struct.PooledConn.html)'s.
/// However you can prepare statements directly on `Pool` without
/// invoking [`Pool::get_conn`](struct.Pool.html#method.get_conn).
///
/// `Pool` will hold at least `min` connections and will create as many as `max`
/// connections with possible overhead of one connection per alive thread.
///
/// Example of multithreaded `Pool` usage:
///
/// ```rust
/// use mysql::{Pool, Opts};
/// # use mysql::OptsBuilder;
/// use std::thread;
///
/// fn get_opts() -> Opts {
///       // ...
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
/// #            .tcp_port(port);
/// #     builder.into()
/// }
///
/// let opts = get_opts();
/// let pool = Pool::new(opts).unwrap();
/// let mut threads = Vec::new();
/// for _ in 0..100 {
///     let pool = pool.clone();
///     threads.push(thread::spawn(move || {
///         let mut result = pool.prep_exec("SELECT 1", ()).unwrap();
///         assert_eq!(result.next().unwrap().unwrap().unwrap(), vec![1.into()]);
///     }));
/// }
/// for t in threads.into_iter() {
///     assert!(t.join().is_ok());
/// }
/// ```
///
/// For more info on how to work with mysql connection please look at
/// [`PooledConn`](struct.PooledConn.html) documentation.
#[derive(Clone)]
pub struct Pool {
    inner: Arc<(Mutex<InnerPool>, Condvar)>,
    min: Arc<AtomicUsize>,
    max: Arc<AtomicUsize>,
    count: Arc<AtomicUsize>,
    check_health: bool,
    use_cache: bool,
}

impl Pool {
    /// Will return connection taken from a pool.
    ///
    /// Will verify and fix it via `Conn::ping` and `Conn::reset` if `call_ping` is `true`.
    /// Will try to get concrete connection if `id` is `Some(_)`.
    /// Will wait til timeout if `timeout_ms` is `Some(_)`
    fn _get_conn<T: AsRef<str>>(
        &self,
        stmt: Option<T>,
        timeout_ms: Option<u32>,
        call_ping: bool,
    ) -> MyResult<PooledConn> {
        let times = if let Some(timeout_ms) = timeout_ms {
            Some((
                SteadyTime::now(),
                Duration::milliseconds(timeout_ms as i64),
                StdDuration::from_millis(timeout_ms as u64),
            ))
        } else {
            None
        };

        let &(ref inner_pool, ref condvar) = &*self.inner;

        let conn = if self.use_cache {
            if let Some(query) = stmt {
                let mut id = None;
                let mut pool = inner_pool.lock()?;
                for (i, conn) in pool.pool.iter().rev().enumerate() {
                    if conn.has_stmt(query.as_ref()) {
                        id = Some(i);
                        break;
                    }
                }
                id.and_then(|id| pool.pool.swap_remove_back(id))
            } else {
                None
            }
        } else {
            None
        };

        let mut conn = if let Some(conn) = conn {
            conn
        } else {
            let out_conn;
            let mut pool = inner_pool.lock()?;
            loop {
                if let Some(conn) = pool.pool.pop_front() {
                    drop(pool);
                    out_conn = Some(conn);
                    break;
                } else {
                    if self.count.load(Ordering::Relaxed) < self.max.load(Ordering::Relaxed) {
                        pool.new_conn()?;
                        self.count.fetch_add(1, Ordering::SeqCst);
                    } else {
                        pool = if let Some((start, timeout, std_timeout)) = times {
                            if SteadyTime::now() - start > timeout {
                                return Err(DriverError::Timeout.into());
                            }
                            condvar.wait_timeout(pool, std_timeout)?.0
                        } else {
                            condvar.wait(pool)?
                        }
                    }
                }
            }
            out_conn.unwrap()
        };

        if call_ping && self.check_health {
            if !conn.ping() {
                conn.reset()?;
            }
        }

        Ok(PooledConn {
            pool: self.clone(),
            conn: Some(conn),
        })
    }

    /// Creates new pool with `min = 10` and `max = 100`.
    pub fn new<T: Into<Opts>>(opts: T) -> MyResult<Pool> {
        Pool::new_manual(10, 100, opts)
    }

    /// Same as `new` but you can set `min` and `max`.
    pub fn new_manual<T: Into<Opts>>(min: usize, max: usize, opts: T) -> MyResult<Pool> {
        let pool = InnerPool::new(min, max, opts.into())?;
        Ok(Pool {
            inner: Arc::new((Mutex::new(pool), Condvar::new())),
            min: Arc::new(AtomicUsize::new(min)),
            max: Arc::new(AtomicUsize::new(max)),
            count: Arc::new(AtomicUsize::new(min)),
            use_cache: true,
            check_health: true,
        })
    }

    /// A way to turn off searching for cached statement (on by default).
    ///
    /// If turned on, then calls to `Pool::{prepare, prep_exec, first_exec}` will search for cached
    /// statement through all connections in the pool. Useless if the value of the `stmt_cache_size`
    /// option is 0.
    pub fn use_cache(&mut self, use_cache: bool) {
        self.use_cache = use_cache;
    }

    /// A way to turn off connection health check on each call to `get_conn` and `prepare`
    /// (`prep_exec` is not affected) (on by default).
    pub fn check_health(&mut self, check_health: bool) {
        self.check_health = check_health;
    }

    /// Gives you a [`PooledConn`](struct.PooledConn.html).
    ///
    /// `Pool` will check that connection is alive via
    /// [`Conn::ping`](struct.Conn.html#method.ping) and will
    /// call [`Conn::reset`](struct.Conn.html#method.reset) if
    /// necessary.
    pub fn get_conn(&self) -> MyResult<PooledConn> {
        self._get_conn(None::<String>, None, true)
    }

    /// Will try to get connection for a duration of `timeout_ms` milliseconds.
    ///
    /// # Failure
    /// This function will return `Error::DriverError(DriverError::Timeout)` if timeout was
    /// reached while waiting for new connection to become available.
    pub fn try_get_conn(&self, timeout_ms: u32) -> MyResult<PooledConn> {
        self._get_conn(None::<String>, Some(timeout_ms), true)
    }

    fn get_conn_by_stmt<T: AsRef<str>>(&self, query: T, call_ping: bool) -> MyResult<PooledConn> {
        self._get_conn(Some(query), None, call_ping)
    }

    /// Will prepare statement. See [`Conn::prepare`](struct.Conn.html#method.prepare).
    ///
    /// It will try to find connection which has this statement cached.
    pub fn prepare<T: AsRef<str>>(&self, query: T) -> MyResult<Stmt<'static>> {
        let conn = self.get_conn_by_stmt(query.as_ref(), true)?;
        conn.pooled_prepare(query)
    }

    /// Shortcut for `pool.get_conn()?.prep_exec(..)`. See
    /// [`Conn::prep_exec`](struct.Conn.html#method.prep_exec).
    ///
    /// It will try to find connection which has this statement cached.
    pub fn prep_exec<A, T>(&self, query: A, params: T) -> MyResult<QueryResult<'static>>
    where
        A: AsRef<str>,
        T: Into<Params>,
    {
        let conn = self.get_conn_by_stmt(query.as_ref(), false)?;
        let params = params.into();
        match conn.pooled_prep_exec(query.as_ref(), params.clone()) {
            Ok(stmt) => Ok(stmt),
            Err(e) => {
                if e.is_connectivity_error() {
                    let conn = self._get_conn(None::<String>, None, true)?;
                    conn.pooled_prep_exec(query, params)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// See [`Conn::first_exec`](struct.Conn.html#method.first_exec).
    pub fn first_exec<Q, P>(&self, query: Q, params: P) -> MyResult<Option<Row>>
    where
        Q: AsRef<str>,
        P: Into<Params>,
    {
        self.prep_exec(query, params).and_then(|result| {
            for row in result {
                return row.map(Some);
            }
            return Ok(None);
        })
    }

    /// Shortcut for `pool.get_conn()?.start_transaction(..)`.
    pub fn start_transaction(
        &self,
        consistent_snapshot: bool,
        isolation_level: Option<IsolationLevel>,
        readonly: Option<bool>,
    ) -> MyResult<Transaction<'static>> {
        let conn = self._get_conn(None::<String>, None, false)?;
        let result = conn.pooled_start_transaction(consistent_snapshot, isolation_level, readonly);
        match result {
            Ok(trans) => Ok(trans),
            Err(ref e) if e.is_connectivity_error() => {
                let conn = self._get_conn(None::<String>, None, true)?;
                conn.pooled_start_transaction(consistent_snapshot, isolation_level, readonly)
            }
            Err(e) => Err(e),
        }
    }
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Pool {{ min: {}, max: {}, count: {} }}",
            self.min.load(Ordering::Relaxed),
            self.max.load(Ordering::Relaxed),
            self.count.load(Ordering::Relaxed)
        )
    }
}

/// Pooled mysql connection which will return to the pool on `drop`.
///
/// You should prefer using `prepare` or `prep_exec` instead of `query` where possible, except
/// cases when statement has no params and when it has no return values or return values which
/// evaluates to `Value::Bytes`.
///
/// `query` is a part of mysql text protocol, so under the hood you will always receive
/// `Value::Bytes` as a result and `from_value` will need to parse it if you want, for example, `i64`
///
/// ```rust
/// # use mysql::{Pool, Opts, OptsBuilder, from_value, Value};
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
/// #            .init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'"]);
/// #     builder.into()
/// # }
/// # let opts = get_opts();
/// # let pool = Pool::new(opts).unwrap();
/// let mut conn = pool.get_conn().unwrap();
///
/// conn.query("SELECT 42").map(|mut result| {
///     let cell = result.next().unwrap().unwrap().take(0).unwrap();
///     assert_eq!(cell, Value::Bytes(b"42".to_vec()));
///     assert_eq!(from_value::<i64>(cell), 42i64);
/// }).unwrap();
/// conn.prep_exec("SELECT 42", ()).map(|mut result| {
///     let cell = result.next().unwrap().unwrap().take(0).unwrap();
///     assert_eq!(cell, Value::Int(42i64));
///     assert_eq!(from_value::<i64>(cell), 42i64);
/// }).unwrap();
/// ```
///
/// For more info on how to work with query results please look at
/// [`QueryResult`](../struct.QueryResult.html) documentation.
#[derive(Debug)]
pub struct PooledConn {
    pool: Pool,
    conn: Option<Conn>,
}

impl Drop for PooledConn {
    fn drop(&mut self) {
        if self.pool.count.load(Ordering::Relaxed) > self.pool.max.load(Ordering::Relaxed)
            || self.conn.is_none()
        {
            self.pool.count.fetch_sub(1, Ordering::SeqCst);
        } else {
            self.conn.as_mut().unwrap().set_local_infile_handler(None);
            let mut pool = (self.pool.inner).0.lock().unwrap();
            pool.pool.push_back(self.conn.take().unwrap());
            drop(pool);
            (self.pool.inner).1.notify_one();
        }
    }
}

impl PooledConn {
    /// Redirects to
    /// [`Conn#query`](struct.Conn.html#method.query).
    pub fn query<T: AsRef<str>>(&mut self, query: T) -> MyResult<QueryResult<'_>> {
        self.conn.as_mut().unwrap().query(query)
    }

    /// See [`Conn::first`](struct.Conn.html#method.first).
    pub fn first<T: AsRef<str>, U: FromRow>(&mut self, query: T) -> MyResult<Option<U>> {
        self.query(query).and_then(|result| {
            for row in result {
                return row.map(|x| Some(from_row(x)));
            }
            return Ok(None);
        })
    }

    /// See [`Conn::prepare`](struct.Conn.html#method.prepare).
    pub fn prepare<T: AsRef<str>>(&mut self, query: T) -> MyResult<Stmt<'_>> {
        self.conn.as_mut().unwrap().prepare(query)
    }

    /// See [`Conn::prep_exec`](struct.Conn.html#method.prep_exec).
    pub fn prep_exec<A, T>(&mut self, query: A, params: T) -> MyResult<QueryResult<'_>>
    where
        A: AsRef<str>,
        T: Into<Params>,
    {
        self.conn.as_mut().unwrap().prep_exec(query, params)
    }

    /// See [`Conn::first_exec`](struct.Conn.html#method.first_exec).
    pub fn first_exec<Q, P, T>(&mut self, query: Q, params: P) -> MyResult<Option<T>>
    where
        Q: AsRef<str>,
        P: Into<Params>,
        T: FromRow,
    {
        self.prep_exec(query, params).and_then(|result| {
            for row in result {
                return row.map(|x| Some(from_row(x)));
            }
            return Ok(None);
        })
    }

    /// Redirects to
    /// [`Conn#start_transaction`](struct.Conn.html#method.start_transaction)
    pub fn start_transaction<'a>(
        &'a mut self,
        consistent_snapshot: bool,
        isolation_level: Option<IsolationLevel>,
        readonly: Option<bool>,
    ) -> MyResult<Transaction<'a>> {
        self.conn.as_mut().unwrap().start_transaction(
            consistent_snapshot,
            isolation_level,
            readonly,
        )
    }

    /// Gives mutable reference to the wrapped
    /// [`Conn`](struct.Conn.html).
    pub fn as_mut<'a>(&'a mut self) -> &'a mut Conn {
        self.conn.as_mut().unwrap()
    }

    /// Gives reference to the wrapped
    /// [`Conn`](struct.Conn.html).
    pub fn as_ref<'a>(&'a self) -> &'a Conn {
        self.conn.as_ref().unwrap()
    }

    /// Unwraps wrapped [`Conn`](struct.Conn.html).
    pub fn unwrap(mut self) -> Conn {
        self.conn.take().unwrap()
    }

    fn pooled_prepare<'a, T: AsRef<str>>(mut self, query: T) -> MyResult<Stmt<'a>> {
        let query = query.as_ref();
        let (named_params, real_query) = parse_named_params(query)?;
        self.as_mut()
            ._prepare(real_query.borrow(), named_params)
            .map(|stmt| Stmt::new_pooled(stmt, self))
    }

    fn pooled_prep_exec<'a, A, T>(mut self, query: A, params: T) -> MyResult<QueryResult<'a>>
    where
        A: AsRef<str>,
        T: Into<Params>,
    {
        let query = query.as_ref();
        let (named_params, real_query) = parse_named_params(query)?;
        let stmt = self.as_mut()._prepare(real_query.borrow(), named_params)?;
        let stmt = Stmt::new_pooled(stmt, self);
        stmt.prep_exec(params)
    }

    fn pooled_start_transaction<'a>(
        mut self,
        consistent_snapshot: bool,
        isolation_level: Option<IsolationLevel>,
        readonly: Option<bool>,
    ) -> MyResult<Transaction<'a>> {
        let _ = self
            .as_mut()
            ._start_transaction(consistent_snapshot, isolation_level, readonly)?;
        Ok(Transaction::new_pooled(self))
    }

    /// A way to override default local infile handler for this pooled connection. Destructor will
    /// restore original handler before returning connection to a pool.
    /// See [`Conn::set_local_infile_handler`](struct.Conn.html#method.set_local_infile_handler).
    pub fn set_local_infile_handler(&mut self, handler: Option<LocalInfileHandler>) {
        self.conn
            .as_mut()
            .unwrap()
            .set_local_infile_handler(handler);
    }
}

impl GenericConnection for PooledConn {
    fn query<T: AsRef<str>>(&mut self, query: T) -> MyResult<QueryResult<'_>> {
        self.query(query)
    }

    fn first<T: AsRef<str>, U: FromRow>(&mut self, query: T) -> MyResult<Option<U>> {
        self.first(query)
    }

    fn prepare<T: AsRef<str>>(&mut self, query: T) -> MyResult<Stmt<'_>> {
        self.prepare(query)
    }

    fn prep_exec<A, T>(&mut self, query: A, params: T) -> MyResult<QueryResult<'_>>
    where
        A: AsRef<str>,
        T: Into<Params>,
    {
        self.prep_exec(query, params)
    }

    fn first_exec<Q, P, T>(&mut self, query: Q, params: P) -> MyResult<Option<T>>
    where
        Q: AsRef<str>,
        P: Into<Params>,
        T: FromRow,
    {
        self.first_exec(query, params)
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod test {
    use crate::Opts;
    use crate::OptsBuilder;

    pub static USER: &'static str = "root";
    pub static PASS: &'static str = "password";
    pub static ADDR: &'static str = "127.0.0.1";
    pub static PORT: u16 = 3307;

    #[cfg(all(feature = "ssl", target_os = "macos"))]
    pub fn get_opts() -> Opts {
        let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or(PASS.to_string());
        let port: u16 = ::std::env::var("MYSQL_SERVER_PORT")
            .ok()
            .map(|my_port| my_port.parse().ok().unwrap_or(PORT))
            .unwrap_or(PORT);
        let mut builder = OptsBuilder::default();
        builder
            .user(Some(USER))
            .pass(Some(pwd))
            .ip_or_hostname(Some(ADDR))
            .tcp_port(port)
            .init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'"])
            .ssl_opts(Some(Some((
                "tests/client.p12",
                "pass",
                vec!["tests/ca-cert.cer"],
            ))));
        builder.into()
    }

    #[cfg(all(feature = "ssl", all(unix, not(target_os = "macos"))))]
    pub fn get_opts() -> Opts {
        let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or(PASS.to_string());
        let port: u16 = ::std::env::var("MYSQL_SERVER_PORT")
            .ok()
            .map(|my_port| my_port.parse().ok().unwrap_or(PORT))
            .unwrap_or(PORT);
        let mut builder = OptsBuilder::default();
        builder
            .user(Some(USER))
            .pass(Some(pwd))
            .ip_or_hostname(Some(ADDR))
            .tcp_port(port)
            .init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'"])
            .ssl_opts(Some(("tests/ca-cert.pem", None::<(String, String)>)));
        builder.into()
    }

    #[cfg(any(not(feature = "ssl"), target_os = "windows"))]
    pub fn get_opts() -> Opts {
        let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or(PASS.to_string());
        let port: u16 = ::std::env::var("MYSQL_SERVER_PORT")
            .ok()
            .map(|my_port| my_port.parse().ok().unwrap_or(PORT))
            .unwrap_or(PORT);
        let mut builder = OptsBuilder::default();
        builder
            .user(Some(USER))
            .pass(Some(pwd))
            .ip_or_hostname(Some(ADDR))
            .tcp_port(port)
            .init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'"]);
        builder.into()
    }

    mod pool {
        use crate::params;
        use crate::OptsBuilder;
        use std::thread;
        use std::time::Duration;

        use super::super::super::super::error::{DriverError, Error};
        use super::super::Pool;
        use super::get_opts;
        use crate::from_row;
        use crate::from_value;

        #[test]
        fn multiple_pools_should_work() {
            let pool = Pool::new(get_opts()).unwrap();
            pool.prep_exec("DROP DATABASE IF EXISTS A", ()).unwrap();
            pool.prep_exec("CREATE DATABASE A", ()).unwrap();
            pool.prep_exec("DROP TABLE IF EXISTS A.a", ()).unwrap();
            pool.prep_exec("CREATE TABLE IF NOT EXISTS A.a (id INT)", ())
                .unwrap();
            pool.prep_exec("INSERT INTO A.a VALUES (1)", ()).unwrap();
            let mut builder = OptsBuilder::from_opts(get_opts());
            builder.db_name(Some("A"));
            let pool2 = Pool::new(builder).unwrap();
            let row = pool2
                .first_exec("SELECT COUNT(*) FROM a", ())
                .unwrap()
                .unwrap();
            assert_eq!((1u8,), from_row(row));
            pool.prep_exec("DROP DATABASE A", ()).unwrap();
        }

        struct A {
            pool: Pool,
            x: u32,
        }

        impl A {
            fn add(&mut self) {
                self.x += 1;
            }
        }

        #[test]
        fn get_opts_from_string() {
            let pass: String =
                ::std::env::var("MYSQL_SERVER_PASS").unwrap_or(super::PASS.to_string());
            let port: u16 = ::std::env::var("MYSQL_SERVER_PORT")
                .ok()
                .map(|my_port| my_port.parse().ok().unwrap_or(super::PORT))
                .unwrap_or(super::PORT);
            Pool::new(format!(
                "mysql://{}:{}@{}:{}",
                super::USER,
                pass,
                super::ADDR,
                port
            ))
            .unwrap();
        }

        #[test]
        fn should_fix_connectivity_errors_on_prepare() {
            let pool = Pool::new_manual(2, 2, get_opts()).unwrap();
            let mut conn = pool.get_conn().unwrap();

            let row = pool
                .first_exec("SELECT CONNECTION_ID();", ())
                .unwrap()
                .unwrap();
            let (id,): (u32,) = from_row(row);

            conn.prep_exec("KILL CONNECTION ?", (id,)).unwrap();
            thread::sleep(Duration::from_millis(250));
            pool.prepare("SHOW FULL PROCESSLIST").unwrap();
        }

        #[test]
        fn should_fix_connectivity_errors_on_prep_exec() {
            let pool = Pool::new_manual(2, 2, get_opts()).unwrap();
            let mut conn = pool.get_conn().unwrap();

            let row = pool
                .first_exec("SELECT CONNECTION_ID();", ())
                .unwrap()
                .unwrap();
            let (id,): (u32,) = from_row(row);

            conn.prep_exec("KILL CONNECTION ?", (id,)).unwrap();
            thread::sleep(Duration::from_millis(250));
            pool.prep_exec("SHOW FULL PROCESSLIST", ()).unwrap();
        }
        #[test]
        fn should_fix_connectivity_errors_on_start_transaction() {
            let pool = Pool::new_manual(2, 2, get_opts()).unwrap();
            let mut conn = pool.get_conn().unwrap();

            let row = pool
                .first_exec("SELECT CONNECTION_ID();", ())
                .unwrap()
                .unwrap();
            let (id,): (u32,) = from_row(row);

            conn.prep_exec("KILL CONNECTION ?", (id,)).unwrap();
            thread::sleep(Duration::from_millis(250));
            pool.start_transaction(false, None, None).unwrap();
        }
        #[test]
        fn should_execute_queryes_on_PooledConn() {
            let pool = Pool::new(get_opts()).unwrap();
            let mut threads = Vec::new();
            for _ in 0usize..10 {
                let pool = pool.clone();
                threads.push(thread::spawn(move || {
                    let conn = pool.get_conn();
                    assert!(conn.is_ok());
                    let mut conn = conn.unwrap();
                    assert!(conn.query("SELECT 1").is_ok());
                }));
            }
            for t in threads.into_iter() {
                assert!(t.join().is_ok());
            }
        }
        #[test]
        fn should_timeout_if_no_connections_available() {
            let pool = Pool::new_manual(0, 1, get_opts()).unwrap();
            let conn1 = pool.try_get_conn(357).unwrap();
            let conn2 = pool.try_get_conn(357);
            assert!(conn2.is_err());
            match conn2 {
                Err(Error::DriverError(DriverError::Timeout)) => assert!(true),
                _ => assert!(false),
            }
            drop(conn1);
            assert!(pool.try_get_conn(357).is_ok());
        }
        #[test]
        fn should_execute_statements_on_PooledConn() {
            let pool = Pool::new(get_opts()).unwrap();
            let mut threads = Vec::new();
            for _ in 0usize..10 {
                let pool = pool.clone();
                threads.push(thread::spawn(move || {
                    let mut conn = pool.get_conn().unwrap();
                    let mut stmt = conn.prepare("SELECT 1").unwrap();
                    assert!(stmt.execute(()).is_ok());
                }));
            }
            for t in threads.into_iter() {
                assert!(t.join().is_ok());
            }

            let pool = Pool::new(get_opts()).unwrap();
            let mut threads = Vec::new();
            for _ in 0usize..10 {
                let pool = pool.clone();
                threads.push(thread::spawn(move || {
                    let mut conn = pool.get_conn().unwrap();
                    conn.prep_exec("SELECT ?", (1,)).unwrap();
                }));
            }
            for t in threads.into_iter() {
                assert!(t.join().is_ok());
            }
        }
        #[test]
        fn should_execute_statements_on_Pool() {
            let pool = Pool::new(get_opts()).unwrap();
            let mut threads = Vec::new();
            for _ in 0usize..10 {
                let pool = pool.clone();
                threads.push(thread::spawn(move || {
                    let mut stmt = pool.prepare("SELECT 1").unwrap();
                    assert!(stmt.execute(()).is_ok());
                }));
            }
            for t in threads.into_iter() {
                assert!(t.join().is_ok());
            }

            let pool = Pool::new(get_opts()).unwrap();
            let mut threads = Vec::new();
            for _ in 0usize..10 {
                let pool = pool.clone();
                threads.push(thread::spawn(move || {
                    pool.prep_exec("SELECT ?", (1,)).unwrap();
                }));
            }
            for t in threads.into_iter() {
                assert!(t.join().is_ok());
            }
            pool.prep_exec("SELECT 1", ())
                .and_then(|mut res1| {
                    pool.prep_exec("SELECT 2", ()).map(|mut res2| {
                        let (x1,) = from_row::<(u8,)>(res1.next().unwrap().unwrap());
                        let (x2,) = from_row::<(u8,)>(res2.next().unwrap().unwrap());
                        assert_eq!(1, x1);
                        assert_eq!(2, x2);
                    })
                })
                .unwrap()
        }
        #[test]
        #[allow(unused_variables)]
        fn should_start_transaction_on_Pool() {
            let pool = Pool::new_manual(1, 10, get_opts()).unwrap();
            pool.prepare("CREATE TEMPORARY TABLE mysql.tbl(a INT)")
                .ok()
                .map(|mut stmt| {
                    assert!(stmt.execute(()).is_ok());
                });
            pool.start_transaction(false, None, None)
                .and_then(|mut t| {
                    t.query("INSERT INTO mysql.tbl(a) VALUES(1)").unwrap();
                    t.query("INSERT INTO mysql.tbl(a) VALUES(2)").unwrap();
                    t.commit()
                })
                .unwrap();
            pool.prepare("SELECT COUNT(a) FROM mysql.tbl")
                .ok()
                .map(|mut stmt| {
                    for x in stmt.execute(()).unwrap() {
                        let mut x = x.unwrap();
                        assert_eq!(from_value::<u8>(x.take(0).unwrap()), 2u8);
                    }
                });
            pool.start_transaction(false, None, None)
                .and_then(|mut t| {
                    t.query("INSERT INTO mysql.tbl(a) VALUES(1)").unwrap();
                    t.query("INSERT INTO mysql.tbl(a) VALUES(2)").unwrap();
                    t.rollback()
                })
                .unwrap();
            pool.prepare("SELECT COUNT(a) FROM mysql.tbl")
                .ok()
                .map(|mut stmt| {
                    for x in stmt.execute(()).unwrap() {
                        let mut x = x.unwrap();
                        assert_eq!(from_value::<u8>(x.take(0).unwrap()), 2u8);
                    }
                });
            pool.start_transaction(false, None, None)
                .and_then(|mut t| {
                    t.query("INSERT INTO mysql.tbl(a) VALUES(1)").unwrap();
                    t.query("INSERT INTO mysql.tbl(a) VALUES(2)").unwrap();
                    Ok(())
                })
                .unwrap();
            pool.prepare("SELECT COUNT(a) FROM mysql.tbl")
                .ok()
                .map(|mut stmt| {
                    for x in stmt.execute(()).unwrap() {
                        let mut x = x.unwrap();
                        assert_eq!(from_value::<u8>(x.take(0).unwrap()), 2u8);
                    }
                });
            let mut a = A { pool: pool, x: 0 };
            let transaction = a.pool.start_transaction(false, None, None).unwrap();
            a.add();
        }
        #[test]
        fn should_start_transaction_on_PooledConn() {
            let pool = Pool::new(get_opts()).unwrap();
            let mut conn = pool.get_conn().unwrap();
            assert!(conn
                .query("CREATE TEMPORARY TABLE mysql.tbl(a INT)")
                .is_ok());
            assert!(conn
                .start_transaction(false, None, None)
                .and_then(|mut t| {
                    assert!(t.query("INSERT INTO mysql.tbl(a) VALUES(1)").is_ok());
                    assert!(t.query("INSERT INTO mysql.tbl(a) VALUES(2)").is_ok());
                    t.commit()
                })
                .is_ok());
            for x in conn.query("SELECT COUNT(a) FROM mysql.tbl").unwrap() {
                let mut x = x.unwrap();
                assert_eq!(from_value::<u8>(x.take(0).unwrap()), 2u8);
            }
            assert!(conn
                .start_transaction(false, None, None)
                .and_then(|mut t| {
                    assert!(t.query("INSERT INTO mysql.tbl(a) VALUES(1)").is_ok());
                    assert!(t.query("INSERT INTO mysql.tbl(a) VALUES(2)").is_ok());
                    t.rollback()
                })
                .is_ok());
            for x in conn.query("SELECT COUNT(a) FROM mysql.tbl").unwrap() {
                let mut x = x.unwrap();
                assert_eq!(from_value::<u8>(x.take(0).unwrap()), 2u8);
            }
            assert!(conn
                .start_transaction(false, None, None)
                .and_then(|mut t| {
                    assert!(t.query("INSERT INTO mysql.tbl(a) VALUES(1)").is_ok());
                    assert!(t.query("INSERT INTO mysql.tbl(a) VALUES(2)").is_ok());
                    Ok(())
                })
                .is_ok());
            for x in conn.query("SELECT COUNT(a) FROM mysql.tbl").unwrap() {
                let mut x = x.unwrap();
                assert_eq!(from_value::<u8>(x.take(0).unwrap()), 2u8);
            }
        }
        #[test]
        fn should_work_with_named_params() {
            let pool = Pool::new(get_opts()).unwrap();
            pool.prepare("SELECT :a, :b, :a + :b, :abc")
                .map(|mut stmt| {
                    let mut result = stmt
                        .execute(params! {
                            "a" => 1,
                            "b" => 2,
                            "abc" => 4,
                        })
                        .unwrap();
                    let row = result.next().unwrap().unwrap();
                    assert_eq!((1, 2, 3, 4), from_row(row));
                })
                .unwrap();

            let params = params! {"a" => 1, "b" => 2, "abc" => 4};
            pool.prep_exec("SELECT :a, :b, :a+:b, :abc", params)
                .map(|mut result| {
                    let row = result.next().unwrap().unwrap();
                    assert_eq!((1, 2, 3, 4), from_row(row));
                })
                .unwrap();
        }

        #[cfg(feature = "nightly")]
        mod bench {
            use super::super::get_opts;
            use std::thread;
            use test;
            use Pool;

            #[bench]
            fn many_prepares(bencher: &mut test::Bencher) {
                let pool = Pool::new(get_opts()).unwrap();
                bencher.iter(|| {
                    pool.prepare("SELECT 1").unwrap();
                });
            }

            #[bench]
            fn many_prepexecs(bencher: &mut test::Bencher) {
                let pool = Pool::new(get_opts()).unwrap();
                bencher.iter(|| {
                    pool.prep_exec("SELECT 1", ()).unwrap();
                });
            }

            #[bench]
            fn many_prepares_threaded(bencher: &mut test::Bencher) {
                let pool = Pool::new(get_opts()).unwrap();
                bencher.iter(|| {
                    let mut threads = Vec::new();
                    for _ in 0..4 {
                        let pool = pool.clone();
                        threads.push(thread::spawn(move || {
                            for _ in 0..250 {
                                test::black_box(
                                    pool.prep_exec(
                                        "SELECT 1, 'hello world', 123.321, ?, ?, ?",
                                        ("hello", "world", 65536),
                                    )
                                    .unwrap(),
                                );
                            }
                        }));
                    }
                    for t in threads {
                        t.join().unwrap();
                    }
                });
            }

            #[bench]
            fn many_prepares_threaded_no_cache(bencher: &mut test::Bencher) {
                let mut pool = Pool::new(get_opts()).unwrap();
                pool.use_cache(false);
                bencher.iter(|| {
                    let mut threads = Vec::new();
                    for _ in 0..4 {
                        let pool = pool.clone();
                        threads.push(thread::spawn(move || {
                            for _ in 0..250 {
                                test::black_box(
                                    pool.prep_exec(
                                        "SELECT 1, 'hello world', 123.321, ?, ?, ?",
                                        ("hello", "world", 65536),
                                    )
                                    .unwrap(),
                                );
                            }
                        }));
                    }
                    for t in threads {
                        t.join().unwrap();
                    }
                });
            }
        }
    }
}
