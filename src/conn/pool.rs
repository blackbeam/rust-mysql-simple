use std::sync::{Arc, Mutex};

use super::IsolationLevel;
use super::Transaction;
use super::super::error::{MyError, DriverError};
use super::{MyConn, MyOpts, Stmt, QueryResult};
use super::super::error::{MyResult};

struct MyInnerPool {
    opts: MyOpts,
    pool: Vec<MyConn>,
    min: usize,
    max: usize,
    count: usize
}

impl MyInnerPool {
    fn new(min: usize, max: usize, opts: MyOpts) -> MyResult<MyInnerPool> {
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
        for _ in (0..min) {
            try!(pool.new_conn());
        }
        Ok(pool)
    }
    fn new_conn(&mut self) -> MyResult<()> {
        match MyConn::new(self.opts.clone()) {
            Ok(conn) => {
                self.pool.push(conn);
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
/// Example of multithreaded `MyPool` usage:
///
/// ```rust
/// use mysql::conn::pool;
/// use std::default::Default;
/// use mysql::conn::MyOpts;
/// use mysql::value::ToValue;
/// use std::thread;
///
/// fn get_opts() -> MyOpts {
///     // ...
/// #     MyOpts {
/// #         user: Some("root".to_string()),
/// #         pass: Some("password".to_string()),
/// #         tcp_addr: Some("127.0.0.1".to_string()),
/// #         tcp_port: 3307,
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
///         let mut stmt = pool.prepare("SELECT 1").unwrap();
///         let mut result = stmt.execute(&[]).unwrap();
///         assert_eq!(result.next().unwrap().unwrap(), vec![1.to_value()])
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
pub struct MyPool {
    pool: Arc<Mutex<MyInnerPool>>
}

impl MyPool {
    /// Creates new pool with `min = 10` and `max = 100`.
    pub fn new(opts: MyOpts) -> MyResult<MyPool> {
        MyPool::new_manual(10, 100, opts)
    }

    /// Same as `new` but you can set `min` and `max`.
    pub fn new_manual(min: usize, max: usize, opts: MyOpts) -> MyResult<MyPool> {
        let pool = try!(MyInnerPool::new(min, max, opts));
        Ok(MyPool{ pool: Arc::new(Mutex::new(pool)) })
    }

    /// Gives you a [`MyPooledConn`](struct.MyPooledConn.html).
    ///
    /// `MyPool` will check that connection is alive via
    /// [`MyConn::ping`](../struct.MyConn.html#method.ping) and will
    /// call [`MyConn::reset`](../struct.MyConn.html#method.reset) if
    /// necessary.
    pub fn get_conn(&self) -> MyResult<MyPooledConn> {
        let mut pool = match self.pool.lock() {
            Ok(mutex) => mutex,
            _ => return Err(MyError::MyDriverError(
                            DriverError::PoisonedPoolMutex)),
        };

        while pool.pool.is_empty() {
            if pool.count < pool.max {
                match pool.new_conn() {
                    Ok(()) => {
                        pool.count += 1;
                        break;
                    },
                    Err(err) => return Err(err)
                }
            }
        }

        let mut conn = pool.pool.pop().unwrap();

        if !conn.ping() {
            try!(conn.reset());
        }

        Ok(MyPooledConn {pool: self.clone(), conn: Some(conn)})
    }

    /// See docs on [`Pool#query`](#method.query)
    pub fn prepare<'a, T: AsRef<str> + 'a>(&'a self, query: T) -> MyResult<Stmt<'a>> {
        let conn = try!(self.get_conn());
        conn.pooled_prepare(query)
    }

    /// Shortcut for `try!(pool.get_conn()).start_transaction(..)`.
    pub fn start_transaction(&self,
                             consistent_snapshot: bool,
                             isolation_level: Option<IsolationLevel>,
                             readonly: Option<bool>) -> MyResult<Transaction> {
        (try!(self.get_conn())).pooled_start_transaction(consistent_snapshot, isolation_level, readonly)
    }
}

/// Pooled mysql connection which will return to the pool at the end of its
/// lifetime.
///
/// You should prefer using `prepare` instead of `query` where possible because
/// of speed and security. `query` is a part of mysql text protocol, so you will
/// always receive `Value::Bytes` as a result.
///
/// ```rust
/// # use mysql::conn::pool;
/// # use mysql::conn::MyOpts;
/// # use mysql::value::Value;
/// # use std::thread::Thread;
/// # use std::default::Default;
/// # fn get_opts() -> MyOpts {
/// #     MyOpts {
/// #         user: Some("root".to_string()),
/// #         pass: Some("password".to_string()),
/// #         tcp_addr: Some("127.0.0.1".to_string()),
/// #         tcp_port: 3307,
/// #         ..Default::default()
/// #     }
/// # }
/// # let opts = get_opts();
/// # let pool = pool::MyPool::new(opts).unwrap();
/// let mut conn = pool.get_conn().unwrap();
///
/// conn.query("SELECT 42").map(|mut result| {
///     assert_eq!(result.next().unwrap().unwrap(), vec![Value::Bytes(b"42".to_vec())]);
/// });
/// conn.prepare("SELECT 42").map(|mut stmt| {
///     let mut result = stmt.execute(&[]).unwrap();
///     assert_eq!(result.next().unwrap().unwrap(), vec![Value::Int(42)]);
/// });
/// ```
///
/// For more info on how to work with query results please look at
/// [`QueryResult`](../struct.QueryResult.html) documentation.
pub struct MyPooledConn {
    pool: MyPool,
    conn: Option<MyConn>
}

impl Drop for MyPooledConn {
    fn drop(&mut self) {
        let mut pool = self.pool.pool.lock().unwrap();
        if pool.count > pool.min || self.conn.is_none() {
            pool.count -= 1;
        } else {
            pool.pool.push(self.conn.take().unwrap());
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
    use conn::MyOpts;
    use std::default::Default;

    pub static USER: &'static str = "root";
    pub static PASS: &'static str = "password";
    pub static ADDR: &'static str = "127.0.0.1";
    pub static PORT: u16          = 3307;

    #[cfg(feature = "openssl")]
    pub fn get_opts() -> MyOpts {
        MyOpts {
            user: Some(USER.to_string()),
            pass: Some(PASS.to_string()),
            tcp_addr: Some(ADDR.to_string()),
            tcp_port: PORT,
            ssl_opts: Some((::std::convert::From::from("tests/ca-cert.pem"), None)),
            ..Default::default()
        }
    }

    #[cfg(not(feature = "ssl"))]
    pub fn get_opts() -> MyOpts {
        MyOpts {
            user: Some(USER.to_string()),
            pass: Some(PASS.to_string()),
            tcp_addr: Some(ADDR.to_string()),
            tcp_port: PORT,
            ..Default::default()
        }
    }

    mod pool {
        use super::get_opts;
        use std::thread;
        use super::super::MyPool;
        use super::super::super::super::value::from_value;
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
        fn should_execute_statements_on_MyPooledConn() {
            let pool = MyPool::new(get_opts()).unwrap();
            let mut threads = Vec::new();
            for _ in 0usize..10 {
                let pool = pool.clone();
                threads.push(thread::spawn(move || {
                    let mut conn = pool.get_conn().unwrap();
                    let mut stmt = conn.prepare("SELECT 1").unwrap();
                    assert!(stmt.execute(&[]).is_ok());
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
                    assert!(stmt.execute(&[]).is_ok());
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
                assert!(stmt.execute(&[]).is_ok());
            });
            assert!(pool.start_transaction(false, None, None).and_then(|mut t| {
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(1)").is_ok());
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(2)").is_ok());
                t.commit()
            }).is_ok());
            pool.prepare("SELECT COUNT(a) FROM x.tbl").ok().map(|mut stmt| {
                for x in stmt.execute(&[]).unwrap() {
                    let x = x.unwrap();
                    assert_eq!(from_value::<u8>(&x[0]), 2u8);
                }
            });
            assert!(pool.start_transaction(false, None, None).and_then(|mut t| {
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(1)").is_ok());
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(2)").is_ok());
                t.rollback()
            }).is_ok());
            pool.prepare("SELECT COUNT(a) FROM x.tbl").ok().map(|mut stmt| {
                for x in stmt.execute(&[]).unwrap() {
                    let x = x.unwrap();
                    assert_eq!(from_value::<u8>(&x[0]), 2u8);
                }
            });
            assert!(pool.start_transaction(false, None, None).and_then(|mut t| {
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(1)").is_ok());
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(2)").is_ok());
                Ok(())
            }).is_ok());
            pool.prepare("SELECT COUNT(a) FROM x.tbl").ok().map(|mut stmt| {
                for x in stmt.execute(&[]).unwrap() {
                    let x = x.unwrap();
                    assert_eq!(from_value::<u8>(&x[0]), 2u8);
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
                let x = x.unwrap();
                assert_eq!(from_value::<u8>(&x[0]), 2u8);
            }
            assert!(conn.start_transaction(false, None, None).and_then(|mut t| {
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(1)").is_ok());
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(2)").is_ok());
                t.rollback()
            }).is_ok());
            for x in conn.query("SELECT COUNT(a) FROM x.tbl").unwrap() {
                let x = x.unwrap();
                assert_eq!(from_value::<u8>(&x[0]), 2u8);
            }
            assert!(conn.start_transaction(false, None, None).and_then(|mut t| {
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(1)").is_ok());
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(2)").is_ok());
                Ok(())
            }).is_ok());
            for x in conn.query("SELECT COUNT(a) FROM x.tbl").unwrap() {
                let x = x.unwrap();
                assert_eq!(from_value::<u8>(&x[0]), 2u8);
            }
        }
    }
}
