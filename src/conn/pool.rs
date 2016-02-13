use std::fmt;
use std::sync::{Arc, Mutex, Condvar};
use std::time::Duration as StdDuration;

use time::{Duration, SteadyTime};

use super::IsolationLevel;
use super::Transaction;
use super::super::error::{MyError, DriverError};
use super::super::value::Params;
use super::{MyConn, Opts, Stmt, QueryResult};
use super::super::error::{MyResult};

#[derive(Debug)]
struct MyInnerPool {
    opts: Opts,
    pool: Vec<MyConn>,
    min: usize,
    max: usize,
    count: usize
}

impl MyInnerPool {
    fn new(min: usize, max: usize, opts: Opts) -> MyResult<MyInnerPool> {
        if min > max || max == 0 {
            return Err(MyError::MyDriverError(DriverError::InvalidPoolConstraints));
        }
        let mut pool = MyInnerPool {
            opts: opts,
            pool: Vec::with_capacity(max),
            max: max,
            min: min,
            count: 0
        };
        for _ in 0..min {
            try!(pool.new_conn());
        }
        Ok(pool)
    }
    fn new_conn(&mut self) -> MyResult<()> {
        match MyConn::new(self.opts.clone()) {
            Ok(conn) => {
                self.pool.push(conn);
                self.count += 1;
                Ok(())
            },
            Err(err) => Err(err)
        }
    }
}

/// `MyPool` serves to provide you with a [`MyPooledConn`](struct.MyPooledConn.html)'s.
/// However you can prepare statements directly on `MyPool` without
/// invoking [`MyPool::get_conn`](struct.MyPool.html#method.get_conn).
///
/// `MyPool` will hold at least `min` connections and will create as many as `max`
/// connections.
///
/// Example of multithreaded `MyPool` usage:
///
/// ```rust
/// use mysql::conn::pool;
/// use std::default::Default;
/// use mysql::conn::Opts;
/// # use mysql::conn::Row;
/// use std::thread;
///
/// fn get_opts() -> Opts {
///       // ...
/// #     let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or("password".to_string());
/// #     let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
/// #                                .map(|my_port| my_port.parse().ok().unwrap_or(3307))
/// #                                .unwrap_or(3307);
/// #     Opts {
/// #         user: Some("root".to_string()),
/// #         pass: Some(pwd),
/// #         ip_or_hostname: Some("127.0.0.1".to_string()),
/// #         tcp_port: port,
/// #         ..Default::default()
/// #     }
/// }
///
/// let opts = get_opts();
/// let pool = pool::MyPool::new(opts).unwrap();
/// let mut threads = Vec::new();
/// for _ in 0..100 {
///     let pool = pool.clone();
///     threads.push(thread::spawn(move || {
///         let mut result = pool.prep_exec("SELECT 1", ()).unwrap();
///         assert_eq!(result.next().unwrap().unwrap(), Row::new(vec![1.into()]));
///     }));
/// }
/// for t in threads.into_iter() {
///     assert!(t.join().is_ok());
/// }
/// ```
///
/// For more info on how to work with mysql connection please look at
/// [`MyPooledConn`](struct.MyPooledConn.html) documentation.
#[derive(Clone)]
pub struct MyPool(Arc<(Mutex<MyInnerPool>, Condvar)>);

impl MyPool {
    /// Creates new pool with `min = 10` and `max = 100`.
    pub fn new<T: Into<Opts>>(opts: T) -> MyResult<MyPool> {
        MyPool::new_manual(10, 100, opts)
    }

    /// Same as `new` but you can set `min` and `max`.
    pub fn new_manual<T: Into<Opts>>(min: usize, max: usize, opts: T) -> MyResult<MyPool> {
        let pool = try!(MyInnerPool::new(min, max, opts.into()));
        Ok(MyPool(Arc::new((Mutex::new(pool), Condvar::new()))))
    }

    /// Gives you a [`MyPooledConn`](struct.MyPooledConn.html).
    ///
    /// `MyPool` will check that connection is alive via
    /// [`MyConn::ping`](../struct.MyConn.html#method.ping) and will
    /// call [`MyConn::reset`](../struct.MyConn.html#method.reset) if
    /// necessary.
    pub fn get_conn(&self) -> MyResult<MyPooledConn> {
        let &(ref inner_pool, ref condvar) = &*self.0;
        let mut pool = match inner_pool.lock() {
            Ok(mutex) => mutex,
            _ => return Err(MyError::MyDriverError(DriverError::PoisonedPoolMutex)),
        };

        loop {
            if pool.pool.is_empty() {
                if pool.count < pool.max {
                    match pool.new_conn() {
                        Ok(()) => break,
                        Err(err) => return Err(err),
                    }
                } else {
                    pool = match condvar.wait(pool) {
                        Ok(mutex) => mutex,
                        _ => return Err(MyError::MyDriverError(DriverError::PoisonedPoolMutex)),
                    }
                }
            } else {
                break;
            }
        }

        let mut conn = pool.pool.pop().unwrap();

        if !conn.ping() {
            try!(conn.reset());
        }

        Ok(MyPooledConn {pool: self.clone(), conn: Some(conn)})
    }

    /// Will try to get connection for a duration of `timeout_ms` milliseconds.
    ///
    /// # Failure
    /// This function will return `MyError::MyDriverError(DriverError::Timeout)` if timeout was
    /// reached while waiting for new connection to become available.
    pub fn try_get_conn(&self, timeout_ms: u32) -> MyResult<MyPooledConn> {
        let start = SteadyTime::now();
        let timeout = Duration::milliseconds(timeout_ms as i64);
        let std_timeout = StdDuration::from_millis(timeout_ms as u64);

        let &(ref inner_pool, ref condvar) = &*self.0;
        let mut pool = match inner_pool.lock() {
            Ok(mutex) => mutex,
            _ => return Err(MyError::MyDriverError(DriverError::PoisonedPoolMutex)),
        };

        loop {
            if pool.pool.is_empty() {
                if pool.count < pool.max {
                    match pool.new_conn() {
                        Ok(()) => break,
                        Err(err) => return Err(err),
                    }
                } else {
                    if SteadyTime::now() - start > timeout {
                        return Err(DriverError::Timeout.into());
                    }
                    pool = match condvar.wait_timeout(pool, std_timeout) {
                        Ok((mutex, _)) => mutex,
                        _ => return Err(MyError::MyDriverError(DriverError::PoisonedPoolMutex)),
                    }
                }
            } else {
                break;
            }
        }

        let mut conn = pool.pool.pop().unwrap();

        if !conn.ping() {
            try!(conn.reset());
        }

        Ok(MyPooledConn {pool: self.clone(), conn: Some(conn)})
    }

    fn get_conn_by_stmt<T: AsRef<str>>(&self, query: T) -> MyResult<MyPooledConn> {
        let conn = {
            let &(ref inner_pool, _) = &*self.0;
            let mut pool = match inner_pool.lock() {
                Ok(mutex) => mutex,
                _ => return Err(MyError::MyDriverError(DriverError::PoisonedPoolMutex)),
            };

            let mut id = None;
            for (i, conn) in pool.pool.iter().enumerate() {
                if conn.has_stmt(query.as_ref()) {
                    id = Some(i);
                    break;
                }
            }

            if let Some(id) = id {
                let mut conn = pool.pool.remove(id);
                if !conn.ping() {
                    try!(conn.reset());
                }
                Some(MyPooledConn {pool: self.clone(), conn: Some(conn)})
            } else {
                None
            }
        };
        match conn {
            Some(pooled_conn) => Ok(pooled_conn),
            None => self.get_conn(),
        }
    }

    /// Will prepare statement.
    ///
    /// It will try to find connection which has this statement cached.
    pub fn prepare<'a, T: AsRef<str> + 'a>(&'a self, query: T) -> MyResult<Stmt<'a>> {
        let conn = try!(self.get_conn_by_stmt(query.as_ref()));
        conn.pooled_prepare(query)
    }

    /// Shortcut for `try!(pool.get_conn()).prep_exec(..)`.
    ///
    /// It will try to find connection which has this statement cached.
    pub fn prep_exec<'a, A: AsRef<str>, T: Into<Params>>(&'a self, query: A, params: T) -> MyResult<QueryResult<'a>> {
        let conn = try!(self.get_conn_by_stmt(query.as_ref()));
        conn.pooled_prep_exec(query, params)
    }

    /// Shortcut for `try!(pool.get_conn()).start_transaction(..)`.
    pub fn start_transaction(&self,
                             consistent_snapshot: bool,
                             isolation_level: Option<IsolationLevel>,
                             readonly: Option<bool>) -> MyResult<Transaction> {
        (try!(self.get_conn())).pooled_start_transaction(consistent_snapshot, isolation_level, readonly)
    }
}

impl fmt::Debug for MyPool {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let pool = (self.0).0.lock().unwrap();
        write!(f, "MyPool {{ min: {}, max: {} }}", pool.min, pool.max)
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
/// # use mysql::conn::pool;
/// # use mysql::conn::Opts;
/// # use mysql::value::{from_value, Value};
/// # use std::thread::Thread;
/// # use std::default::Default;
/// # fn get_opts() -> Opts {
/// #     let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or("password".to_string());
/// #     let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
/// #                                .map(|my_port| my_port.parse().ok().unwrap_or(3307))
/// #                                .unwrap_or(3307);
/// #     Opts {
/// #         user: Some("root".to_string()),
/// #         pass: Some(pwd),
/// #         ip_or_hostname: Some("127.0.0.1".to_string()),
/// #         tcp_port: port,
/// #         ..Default::default()
/// #     }
/// # }
/// # let opts = get_opts();
/// # let pool = pool::MyPool::new(opts).unwrap();
/// let mut conn = pool.get_conn().unwrap();
///
/// conn.query("SELECT 42").map(|mut result| {
///     let cell = result.next().unwrap().unwrap().take(0).unwrap();
///     assert_eq!(cell, Value::Bytes(b"42".to_vec()));
///     assert_eq!(from_value::<i64>(cell), 42i64);
/// });
/// conn.prep_exec("SELECT 42", ()).map(|mut result| {
///     let cell = result.next().unwrap().unwrap().take(0).unwrap();
///     assert_eq!(cell, Value::Int(42i64));
///     assert_eq!(from_value::<i64>(cell), 42i64);
/// });
/// ```
///
/// For more info on how to work with query results please look at
/// [`QueryResult`](../struct.QueryResult.html) documentation.
#[derive(Debug)]
pub struct MyPooledConn {
    pool: MyPool,
    conn: Option<MyConn>
}

impl Drop for MyPooledConn {
    fn drop(&mut self) {
        let mut pool = (self.pool.0).0.lock().unwrap();
        if pool.count > pool.min || self.conn.is_none() {
            pool.count -= 1;
        } else {
            pool.pool.push(self.conn.take().unwrap());
            (self.pool.0).1.notify_one();
        }
    }
}

impl MyPooledConn {
    /// Redirects to
    /// [`MyConn#query`](../struct.MyConn.html#method.query).
    pub fn query<'a, T: AsRef<str> + 'a>(&'a mut self, query: T) -> MyResult<QueryResult<'a>> {
        self.conn.as_mut().unwrap().query(query)
    }

    /// Redirects to
    /// [`MyConn#prepare`](../struct.MyConn.html#method.prepare).
    pub fn prepare<'a, T: AsRef<str> + 'a>(&'a mut self, query: T) -> MyResult<Stmt<'a>> {
        self.conn.as_mut().unwrap().prepare(query)
    }

    /// Redirects to
    /// [`MyConn#prep_exec`](../struct.MyConn.html#method.prep_exec).
    pub fn prep_exec<'a, A: AsRef<str> + 'a, T: Into<Params>>(&'a mut self, query: A, params: T) -> MyResult<QueryResult<'a>> {
        self.conn.as_mut().unwrap().prep_exec(query, params)
    }

    /// Redirects to
    /// [`MyConn#start_transaction`](../struct.MyConn.html#method.start_transaction)
    pub fn start_transaction<'a>(&'a mut self,
                                 consistent_snapshot: bool,
                                 isolation_level: Option<IsolationLevel>,
                                 readonly: Option<bool>) -> MyResult<Transaction<'a>> {
        self.conn.as_mut().unwrap().start_transaction(consistent_snapshot,
                                                      isolation_level,
                                                      readonly)
    }

    /// Gives mutable reference to the wrapped
    /// [`MyConn`](../struct.MyConn.html).
    pub fn as_mut<'a>(&'a mut self) -> &'a mut MyConn {
        self.conn.as_mut().unwrap()
    }

    /// Gives reference to the wrapped
    /// [`MyConn`](../struct.MyConn.html).
    pub fn as_ref<'a>(&'a self) -> &'a MyConn {
        self.conn.as_ref().unwrap()
    }

    /// Unwraps wrapped [`MyConn`](../struct.MyConn.html).
    pub fn unwrap(mut self) -> MyConn {
        self.conn.take().unwrap()
    }

    fn pooled_prepare<'a, T: AsRef<str>>(mut self, query: T) -> MyResult<Stmt<'a>> {
        match self.as_mut()._prepare(query.as_ref()) {
            Ok(stmt) => Ok(Stmt::new_pooled(stmt, self)),
            Err(err) => Err(err)
        }
    }

    fn pooled_prep_exec<'a, A, T>(mut self, query: A, params: T) -> MyResult<QueryResult<'a>>
    where A: AsRef<str>,
          T: Into<Params>
    {
        let stmt = try!(self.as_mut()._prepare(query.as_ref()));
        let stmt = Stmt::new_pooled(stmt, self);
        stmt.prep_exec(params)
    }

    fn pooled_start_transaction<'a>(mut self,
                                    consistent_snapshot: bool,
                                    isolation_level: Option<IsolationLevel>,
                                    readonly: Option<bool>) -> MyResult<Transaction<'a>> {
        let _ = try!(self.as_mut()._start_transaction(consistent_snapshot,
                                                      isolation_level,
                                                      readonly));
        Ok(Transaction::new_pooled(self))
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod test {
    use conn::Opts;
    use std::default::Default;

    pub static USER: &'static str = "root";
    pub static PASS: &'static str = "password";
    pub static ADDR: &'static str = "127.0.0.1";
    pub static PORT: u16          = 3307;

    #[cfg(feature = "openssl")]
    pub fn get_opts() -> Opts {
        let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or(PASS.to_string());
        let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
                                   .map(|my_port| my_port.parse().ok().unwrap_or(PORT))
                                   .unwrap_or(PORT);
        Opts {
            user: Some(USER.to_string()),
            pass: Some(pwd),
            ip_or_hostname: Some(ADDR.to_string()),
            tcp_port: port,
            ssl_opts: Some((::std::convert::From::from("tests/ca-cert.pem"), None)),
            ..Default::default()
        }
    }

    #[cfg(not(feature = "ssl"))]
    pub fn get_opts() -> Opts {
        let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or(PASS.to_string());
        let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
                                   .map(|my_port| my_port.parse().ok().unwrap_or(PORT))
                                   .unwrap_or(PORT);
        Opts {
            user: Some(USER.to_string()),
            pass: Some(pwd),
            ip_or_hostname: Some(ADDR.to_string()),
            tcp_port: port,
            ..Default::default()
        }
    }

    mod pool {
        use super::get_opts;
        use std::thread;
        use super::super::MyPool;
        use super::super::super::super::value::from_value;
        use super::super::super::super::error::{MyError, DriverError};
        #[test]
        fn should_execute_queryes_on_MyPooledConn() {
            let pool = MyPool::new(get_opts()).unwrap();
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
            let pool = MyPool::new_manual(0, 1, get_opts()).unwrap();
            let conn1 = pool.try_get_conn(357).unwrap();
            let conn2 = pool.try_get_conn(357);
            assert!(conn2.is_err());
            match conn2 {
                Err(MyError::MyDriverError(DriverError::Timeout)) => assert!(true),
                _ => assert!(false),
            }
            drop(conn1);
            assert!(pool.try_get_conn(357).is_ok());
        }
        #[test]
        fn should_execute_statements_on_MyPooledConn() {
            let pool = MyPool::new(get_opts()).unwrap();
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

            let pool = MyPool::new(get_opts()).unwrap();
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
        fn should_execute_statements_on_MyPool() {
            let pool = MyPool::new(get_opts()).unwrap();
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

            let pool = MyPool::new(get_opts()).unwrap();
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
        }
        #[test]
        fn should_start_transaction_on_MyPool() {
            let pool = MyPool::new(get_opts()).unwrap();
            pool.prepare("CREATE TEMPORARY TABLE x.tbl(a INT)").ok().map(|mut stmt| {
                assert!(stmt.execute(()).is_ok());
            });
            assert!(pool.start_transaction(false, None, None).and_then(|mut t| {
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(1)").is_ok());
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(2)").is_ok());
                t.commit()
            }).is_ok());
            pool.prepare("SELECT COUNT(a) FROM x.tbl").ok().map(|mut stmt| {
                for x in stmt.execute(()).unwrap() {
                    let mut x = x.unwrap();
                    assert_eq!(from_value::<u8>(x.take(0).unwrap()), 2u8);
                }
            });
            assert!(pool.start_transaction(false, None, None).and_then(|mut t| {
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(1)").is_ok());
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(2)").is_ok());
                t.rollback()
            }).is_ok());
            pool.prepare("SELECT COUNT(a) FROM x.tbl").ok().map(|mut stmt| {
                for x in stmt.execute(()).unwrap() {
                    let mut x = x.unwrap();
                    assert_eq!(from_value::<u8>(x.take(0).unwrap()), 2u8);
                }
            });
            assert!(pool.start_transaction(false, None, None).and_then(|mut t| {
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(1)").is_ok());
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(2)").is_ok());
                Ok(())
            }).is_ok());
            pool.prepare("SELECT COUNT(a) FROM x.tbl").ok().map(|mut stmt| {
                for x in stmt.execute(()).unwrap() {
                    let mut x = x.unwrap();
                    assert_eq!(from_value::<u8>(x.take(0).unwrap()), 2u8);
                }
            });
        }
        #[test]
        fn should_start_transaction_on_MyPooledConn() {
            let pool = MyPool::new(get_opts()).unwrap();
            let mut conn = pool.get_conn().unwrap();
            assert!(conn.query("CREATE TEMPORARY TABLE x.tbl(a INT)").is_ok());
            assert!(conn.start_transaction(false, None, None).and_then(|mut t| {
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(1)").is_ok());
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(2)").is_ok());
                t.commit()
            }).is_ok());
            for x in conn.query("SELECT COUNT(a) FROM x.tbl").unwrap() {
                let mut x = x.unwrap();
                assert_eq!(from_value::<u8>(x.take(0).unwrap()), 2u8);
            }
            assert!(conn.start_transaction(false, None, None).and_then(|mut t| {
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(1)").is_ok());
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(2)").is_ok());
                t.rollback()
            }).is_ok());
            for x in conn.query("SELECT COUNT(a) FROM x.tbl").unwrap() {
                let mut x = x.unwrap();
                assert_eq!(from_value::<u8>(x.take(0).unwrap()), 2u8);
            }
            assert!(conn.start_transaction(false, None, None).and_then(|mut t| {
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(1)").is_ok());
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(2)").is_ok());
                Ok(())
            }).is_ok());
            for x in conn.query("SELECT COUNT(a) FROM x.tbl").unwrap() {
                let mut x = x.unwrap();
                assert_eq!(from_value::<u8>(x.take(0).unwrap()), 2u8);
            }
        }
    }
}
