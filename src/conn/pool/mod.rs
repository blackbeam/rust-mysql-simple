// Copyright (c) 2020 rust-mysql-simple contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{
    fmt,
    ops::Deref,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    conn::query_result::{Binary, Text},
    prelude::*,
    ChangeUserOpts, Conn, DriverError, LocalInfileHandler, Opts, Params, QueryResult, Result,
    Statement, Transaction, TxOpts,
};

mod inner;

/// Thread-safe cloneable smart pointer to a connection pool.
///
/// However you can prepare statements directly on `Pool` without
/// invoking [`Pool::get_conn`](struct.Pool.html#method.get_conn).
///
/// `Pool` will hold at least `min` connections and will create as many as `max`
/// connections with possible overhead of one connection per alive thread.
///
/// Example of multithreaded `Pool` usage:
///
/// ```rust
/// # mysql::doctest_wrapper!(__result, {
/// # use mysql::*;
/// # use mysql::prelude::*;
/// # let mut conn = Conn::new(get_opts())?;
/// # let pool_opts = PoolOpts::new().with_constraints(PoolConstraints::new_const::<5, 10>());
/// # let opts = get_opts().pool_opts(pool_opts);
/// let pool = Pool::new(opts).unwrap();
/// let mut threads = Vec::new();
///
/// for _ in 0..1000 {
///     let pool = pool.clone();
///     threads.push(std::thread::spawn(move || {
///         let mut conn = pool.get_conn().unwrap();
///         let result: u8 = conn.query_first("SELECT 1").unwrap().unwrap();
///         assert_eq!(result, 1_u8);
///     }));
/// }
///
/// for t in threads.into_iter() {
///     assert!(t.join().is_ok());
/// }
/// # });
/// ```
///
/// For more info on how to work with mysql connection please look at
/// [`PooledConn`](struct.PooledConn.html) documentation.
#[derive(Clone)]
pub struct Pool {
    inner: Arc<inner::Inner>,
}

impl Pool {
    /// Will return connection taken from a pool.
    ///
    /// Will wait til timeout if `timeout_ms` is `Some(_)`
    fn _get_conn<T: AsRef<[u8]>>(
        &self,
        stmt: Option<T>,
        timeout: Option<Duration>,
        mut call_ping: bool,
    ) -> Result<PooledConn> {
        let times = timeout.map(|timeout| (Instant::now(), timeout));

        let (protected, condvar) = self.inner.protected();

        let conn = if !self.inner.opts().reset_connection() {
            // stmt cache considered enabled if reset_connection is false
            if let Some(ref query) = stmt {
                protected.lock()?.take_by_query(query.as_ref())
            } else {
                None
            }
        } else {
            None
        };

        let mut conn = if let Some(conn) = conn {
            conn
        } else {
            let mut protected = protected.lock()?;
            loop {
                if let Some(conn) = protected.pop_front() {
                    drop(protected);
                    break conn;
                } else if self.inner.is_full() {
                    protected = if let Some((start, timeout)) = times {
                        if start.elapsed() > timeout {
                            return Err(DriverError::Timeout.into());
                        }
                        condvar.wait_timeout(protected, timeout)?.0
                    } else {
                        condvar.wait(protected)?
                    }
                } else {
                    protected.new_conn()?;
                    self.inner.increase();
                    // we do not have to call ping for a fresh connection
                    call_ping = false;
                }
            }
        };

        if call_ping && self.inner.opts().check_health() && conn.ping().is_err() {
            // existing connection seem to be dead, retrying..
            self.inner.decrease();
            return self._get_conn(stmt, timeout, call_ping);
        }

        Ok(PooledConn {
            pool: self.clone(),
            conn: Some(conn),
        })
    }

    /// Creates new pool with the given options (see [`Opts`]).
    pub fn new<T, E>(opts: T) -> Result<Pool>
    where
        Opts: TryFrom<T, Error = E>,
        crate::Error: From<E>,
    {
        Ok(Pool {
            inner: Arc::new(inner::Inner::new(Opts::try_from(opts)?)?),
        })
    }

    /// Gives you a [`PooledConn`](struct.PooledConn.html).
    pub fn get_conn(&self) -> Result<PooledConn> {
        self._get_conn(None::<String>, None, true)
    }

    /// Will try to get connection for the duration of `timeout`.
    ///
    /// # Failure
    /// This function will return `Error::DriverError(DriverError::Timeout)` if timeout was
    /// reached while waiting for new connection to become available.
    pub fn try_get_conn(&self, timeout: Duration) -> Result<PooledConn> {
        self._get_conn(None::<String>, Some(timeout), true)
    }

    /// Shortcut for `pool.get_conn()?.start_transaction(..)`.
    pub fn start_transaction(&self, tx_opts: TxOpts) -> Result<Transaction<'static>> {
        let conn = self._get_conn(None::<String>, None, false)?;
        let result = conn.pooled_start_transaction(tx_opts);
        match result {
            Ok(trans) => Ok(trans),
            Err(ref e) if e.is_connectivity_error() => {
                let conn = self._get_conn(None::<String>, None, true)?;
                conn.pooled_start_transaction(tx_opts)
            }
            Err(e) => Err(e),
        }
    }
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Pool {{ constraints: {:?}, count: {} }}",
            self.inner.opts().constraints(),
            self.inner.count(),
        )
    }
}

/// Pooled mysql connection.
///
/// You should prefer using `prep` along `exec` instead of `query` from the Queryable trait where
/// possible, except cases when statement has no params and when it has no return values or return
/// values which evaluates to `Value::Bytes`.
///
/// `query` is a part of mysql text protocol, so under the hood you will always receive
/// `Value::Bytes` as a result and `from_value` will need to parse it if you want, for example, `i64`
///
/// ```rust
/// # mysql::doctest_wrapper!(__result, {
/// # use mysql::*;
/// # use mysql::prelude::*;
/// # let mut conn = Conn::new(get_opts())?;
/// let pool = Pool::new(get_opts()).unwrap();
/// let mut conn = pool.get_conn().unwrap();
///
/// conn.query_first("SELECT 42").map(|result: Option<Value>| {
///     let result = result.unwrap();
///     assert_eq!(result, Value::Bytes(b"42".to_vec()));
///     assert_eq!(from_value::<i64>(result), 42i64);
/// }).unwrap();
/// conn.exec_iter("SELECT 42", ()).map(|mut result| {
///     let cell = result.next().unwrap().unwrap().take(0).unwrap();
///     assert_eq!(cell, Value::Int(42i64));
///     assert_eq!(from_value::<i64>(cell), 42i64);
/// }).unwrap();
/// # });
/// ```
///
/// For more info on how to work with query results please look at
/// [`QueryResult`](../struct.QueryResult.html) documentation.
#[derive(Debug)]
pub struct PooledConn {
    pool: Pool,
    conn: Option<Conn>,
}

impl Deref for PooledConn {
    type Target = Conn;

    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().expect("deref after drop")
    }
}

impl Drop for PooledConn {
    fn drop(&mut self) {
        if let Some(mut conn) = self.conn.take() {
            match conn.cleanup_for_pool() {
                Ok(_) => {
                    let (protected, condvar) = self.pool.inner.protected();
                    match protected.lock() {
                        Ok(mut protected) => {
                            protected.push_back(conn);
                            drop(protected);
                            condvar.notify_one();
                        }
                        Err(_) => {
                            // everything is broken
                            self.pool.inner.decrease();
                        }
                    }
                }
                Err(_) => {
                    // the connection is broken
                    self.pool.inner.decrease();
                }
            }
        }
    }
}

impl PooledConn {
    /// Redirects to
    /// [`Conn#start_transaction`](struct.Conn.html#method.start_transaction)
    pub fn start_transaction(&mut self, tx_opts: TxOpts) -> Result<Transaction<'_>> {
        self.conn.as_mut().unwrap().start_transaction(tx_opts)
    }

    /// Turns this connection into a binlog stream (see [`Conn::get_binlog_stream`]).
    #[cfg(feature = "binlog")]
    #[cfg_attr(docsrs, doc(cfg(feature = "binlog")))]
    pub fn get_binlog_stream(
        mut self,
        request: crate::BinlogRequest<'_>,
    ) -> Result<crate::BinlogStream> {
        self.conn.take().unwrap().get_binlog_stream(request)
    }

    /// Unwraps wrapped [`Conn`](struct.Conn.html).
    pub fn unwrap(mut self) -> Conn {
        self.conn.take().unwrap()
    }

    fn pooled_start_transaction(mut self, tx_opts: TxOpts) -> Result<Transaction<'static>> {
        self.as_mut()._start_transaction(tx_opts)?;
        Ok(Transaction::new(self.into()))
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

    /// Invokes `COM_CHANGE_USER` (see [`Conn::change_user`] docs).
    pub fn change_user(&mut self) -> Result<()> {
        self.conn
            .as_mut()
            .unwrap()
            .change_user(ChangeUserOpts::default())
    }

    /// Turns on/off automatic connection reset upon return to a pool (see [`Opts::get_pool_opts`]).
    ///
    /// Initial value is taken from [`crate::PoolOpts::reset_connection`].
    pub fn reset_connection(&mut self, reset_connection: bool) {
        if let Some(conn) = self.conn.as_mut() {
            conn.0.reset_upon_return = reset_connection;
        }
    }
}

impl AsRef<Conn> for PooledConn {
    fn as_ref(&self) -> &Conn {
        self.conn.as_ref().unwrap()
    }
}

impl AsMut<Conn> for PooledConn {
    fn as_mut(&mut self) -> &mut Conn {
        self.conn.as_mut().unwrap()
    }
}

impl Queryable for PooledConn {
    fn query_iter<T: AsRef<str>>(&mut self, query: T) -> Result<QueryResult<'_, '_, '_, Text>> {
        self.conn.as_mut().unwrap().query_iter(query)
    }

    fn prep<T: AsRef<str>>(&mut self, query: T) -> Result<Statement> {
        self.conn.as_mut().unwrap().prep(query)
    }

    fn close(&mut self, stmt: Statement) -> Result<()> {
        self.conn.as_mut().unwrap().close(stmt)
    }

    fn exec_iter<S, P>(&mut self, stmt: S, params: P) -> Result<QueryResult<'_, '_, '_, Binary>>
    where
        S: AsStatement,
        P: Into<Params>,
    {
        self.conn.as_mut().unwrap().exec_iter(stmt, params)
    }

    fn exec_batch<S, P, I>(&mut self, stmt: S, params: I) -> Result<()>
    where
        Self: Sized,
        S: AsStatement,
        P: Into<Params>,
        I: IntoIterator<Item = P>,
    {
        self.conn.as_mut().unwrap().exec_batch(stmt, params)
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod test {
    mod pool {
        use std::{thread, time::Duration};

        use crate::{
            from_value, prelude::*, test_misc::get_opts, DriverError, Error, OptsBuilder, Pool,
            PoolConstraints, PoolOpts, TxOpts, Value,
        };

        #[test]
        fn multiple_pools_should_work() {
            let pool = Pool::new(get_opts()).unwrap();
            pool.get_conn()
                .unwrap()
                .exec_drop("DROP DATABASE IF EXISTS A", ())
                .unwrap();
            pool.get_conn()
                .unwrap()
                .exec_drop("CREATE DATABASE A", ())
                .unwrap();
            pool.get_conn()
                .unwrap()
                .exec_drop("DROP TABLE IF EXISTS A.a", ())
                .unwrap();
            pool.get_conn()
                .unwrap()
                .exec_drop("CREATE TABLE IF NOT EXISTS A.a (id INT)", ())
                .unwrap();
            pool.get_conn()
                .unwrap()
                .exec_drop("INSERT INTO A.a VALUES (1)", ())
                .unwrap();
            let opts = OptsBuilder::from_opts(get_opts()).db_name(Some("A"));
            let pool2 = Pool::new(opts).unwrap();
            let count: u8 = pool2
                .get_conn()
                .unwrap()
                .exec_first("SELECT COUNT(*) FROM a", ())
                .unwrap()
                .unwrap();
            assert_eq!(1, count);
            pool.get_conn()
                .unwrap()
                .exec_drop("DROP DATABASE A", ())
                .unwrap();
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
        fn should_fix_connectivity_errors_on_prepare() {
            let pool = Pool::new(get_opts().pool_opts(
                PoolOpts::default().with_constraints(PoolConstraints::new_const::<2, 2>()),
            ))
            .unwrap();
            let mut conn = pool.get_conn().unwrap();

            let id: u32 = pool
                .get_conn()
                .unwrap()
                .exec_first("SELECT CONNECTION_ID();", ())
                .unwrap()
                .unwrap();

            conn.query_drop(&*format!("KILL {}", id)).unwrap();
            thread::sleep(Duration::from_millis(250));
            pool.get_conn()
                .unwrap()
                .prep("SHOW FULL PROCESSLIST")
                .unwrap();
        }

        #[test]
        fn should_fix_connectivity_errors_on_prep_exec() {
            let pool = Pool::new(get_opts().pool_opts(
                PoolOpts::default().with_constraints(PoolConstraints::new_const::<2, 2>()),
            ))
            .unwrap();
            let mut conn = pool.get_conn().unwrap();

            let id: u32 = pool
                .get_conn()
                .unwrap()
                .exec_first("SELECT CONNECTION_ID();", ())
                .unwrap()
                .unwrap();

            conn.query_drop(&*format!("KILL {}", id)).unwrap();
            thread::sleep(Duration::from_millis(250));
            pool.get_conn()
                .unwrap()
                .exec_drop("SHOW FULL PROCESSLIST", ())
                .unwrap();
        }
        #[test]
        fn should_fix_connectivity_errors_on_start_transaction() {
            let pool = Pool::new(get_opts().pool_opts(
                PoolOpts::default().with_constraints(PoolConstraints::new_const::<2, 2>()),
            ))
            .unwrap();
            let mut conn = pool.get_conn().unwrap();

            let id: u32 = pool
                .get_conn()
                .unwrap()
                .exec_first("SELECT CONNECTION_ID();", ())
                .unwrap()
                .unwrap();

            conn.query_drop(&*format!("KILL {}", id)).unwrap();
            thread::sleep(Duration::from_millis(250));
            pool.start_transaction(TxOpts::default()).unwrap();
        }
        #[test]
        fn should_execute_queries_on_PooledConn() {
            let pool = Pool::new(get_opts()).unwrap();
            let mut threads = Vec::new();
            for _ in 0usize..10 {
                let pool = pool.clone();
                threads.push(thread::spawn(move || {
                    let conn = pool.get_conn();
                    assert!(conn.is_ok());
                    let mut conn = conn.unwrap();
                    conn.query_drop("SELECT 1").unwrap();
                }));
            }
            for t in threads.into_iter() {
                assert!(t.join().is_ok());
            }
        }
        #[test]
        fn should_timeout_if_no_connections_available() {
            let pool = Pool::new(get_opts().pool_opts(
                PoolOpts::default().with_constraints(PoolConstraints::new_const::<0, 1>()),
            ))
            .unwrap();
            let conn1 = pool.try_get_conn(Duration::from_millis(357)).unwrap();
            let conn2 = pool.try_get_conn(Duration::from_millis(357));
            assert!(conn2.is_err());
            match conn2 {
                Err(Error::DriverError(DriverError::Timeout)) => (),
                _ => panic!("Timeout error expected"),
            }
            drop(conn1);
            assert!(pool.try_get_conn(Duration::from_millis(357)).is_ok());
        }

        #[test]
        fn should_be_none_if_pool_size_zero_zero() {
            let pool_constraints = PoolConstraints::new(0, 0);
            assert!(pool_constraints.is_none());
        }

        #[test]
        #[should_panic]
        fn should_panic_if_pool_size_zero_zero() {
            PoolConstraints::new_const::<0, 0>();
        }

        #[test]
        fn should_execute_statements_on_PooledConn() {
            let pool = Pool::new(get_opts()).unwrap();
            let mut threads = Vec::new();
            for _ in 0usize..10 {
                let pool = pool.clone();
                threads.push(thread::spawn(move || {
                    let mut conn = pool.get_conn().unwrap();
                    let stmt = conn.prep("SELECT 1").unwrap();
                    conn.exec_drop(&stmt, ()).unwrap();
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
                    conn.exec_drop("SELECT ?", (1,)).unwrap();
                }));
            }
            for t in threads.into_iter() {
                assert!(t.join().is_ok());
            }
        }

        #[test]
        #[allow(unused_variables)]
        fn should_start_transaction_on_Pool() {
            let pool = Pool::new(
                get_opts().pool_opts(
                    PoolOpts::default()
                        .with_constraints(PoolConstraints::new_const::<1, 10>())
                        .with_reset_connection(false),
                ),
            )
            .unwrap();
            pool.get_conn()
                .unwrap()
                .query_drop("CREATE TEMPORARY TABLE mysql.tbl(a INT)")
                .unwrap();
            pool.start_transaction(TxOpts::default())
                .and_then(|mut t| {
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(1)").unwrap();
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(2)").unwrap();
                    t.commit()
                })
                .unwrap();
            assert_eq!(
                pool.get_conn()
                    .unwrap()
                    .query_first::<u8, _>("SELECT COUNT(a) FROM mysql.tbl")
                    .unwrap()
                    .unwrap(),
                2_u8
            );
            pool.start_transaction(TxOpts::default())
                .and_then(|mut t| {
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(1)").unwrap();
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(2)").unwrap();
                    t.rollback()
                })
                .unwrap();
            assert_eq!(
                pool.get_conn()
                    .unwrap()
                    .query_first::<u8, _>("SELECT COUNT(a) FROM mysql.tbl")
                    .unwrap()
                    .unwrap(),
                2_u8
            );
            pool.start_transaction(TxOpts::default())
                .map(|mut t| {
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(1)").unwrap();
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(2)").unwrap();
                })
                .unwrap();
            assert_eq!(
                pool.get_conn()
                    .unwrap()
                    .query_first::<u8, _>("SELECT COUNT(a) FROM mysql.tbl")
                    .unwrap()
                    .unwrap(),
                2_u8
            );
            let mut a = A { pool, x: 0 };
            let transaction = a.pool.start_transaction(TxOpts::default()).unwrap();
            a.add();
        }

        #[test]
        fn should_reuse_connections() -> crate::Result<()> {
            let pool = Pool::new(get_opts().pool_opts(
                PoolOpts::default().with_constraints(PoolConstraints::new_const::<1, 1>()),
            ))?;
            let mut conn = pool.get_conn()?;

            let server_version = conn.server_version();
            let connection_id = conn.connection_id();

            for _ in 0..16 {
                drop(conn);
                conn = pool.get_conn()?;
                println!("CONN connection_id={}", conn.connection_id());
                assert!(conn.connection_id() == connection_id || server_version < (5, 7, 2));
            }

            Ok(())
        }

        #[test]
        fn should_start_transaction_on_PooledConn() {
            let pool = Pool::new(get_opts()).unwrap();
            let mut conn = pool.get_conn().unwrap();
            conn.query_drop("CREATE TEMPORARY TABLE mysql.tbl(a INT)")
                .unwrap();
            conn.start_transaction(TxOpts::default())
                .and_then(|mut t| {
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(1)").unwrap();
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(2)").unwrap();
                    t.commit()
                })
                .unwrap();
            for x in conn.query_iter("SELECT COUNT(a) FROM mysql.tbl").unwrap() {
                let mut x = x.unwrap();
                assert_eq!(from_value::<u8>(x.take(0).unwrap()), 2u8);
            }
            conn.start_transaction(TxOpts::default())
                .and_then(|mut t| {
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(1)").unwrap();
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(2)").unwrap();
                    t.rollback()
                })
                .unwrap();
            for x in conn.query_iter("SELECT COUNT(a) FROM mysql.tbl").unwrap() {
                let mut x = x.unwrap();
                assert_eq!(from_value::<u8>(x.take(0).unwrap()), 2u8);
            }
            conn.start_transaction(TxOpts::default())
                .map(|mut t| {
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(1)").unwrap();
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(2)").unwrap();
                })
                .unwrap();
            for x in conn.query_iter("SELECT COUNT(a) FROM mysql.tbl").unwrap() {
                let mut x = x.unwrap();
                assert_eq!(from_value::<u8>(x.take(0).unwrap()), 2u8);
            }
        }

        #[test]
        fn should_opt_out_of_connection_reset() {
            let pool_opts = PoolOpts::new().with_constraints(PoolConstraints::new_const::<1, 1>());
            let opts = get_opts().pool_opts(pool_opts.clone());

            let pool = Pool::new(opts.clone()).unwrap();

            let mut conn = pool.get_conn().unwrap();
            assert_eq!(
                conn.query_first::<Value, _>("SELECT @foo").unwrap(),
                Some(Value::NULL)
            );
            conn.query_drop("SET @foo = 'foo'").unwrap();
            assert_eq!(
                conn.query_first::<String, _>("SELECT @foo")
                    .unwrap()
                    .unwrap(),
                "foo",
            );
            drop(conn);

            conn = pool.get_conn().unwrap();
            assert_eq!(
                conn.query_first::<Value, _>("SELECT @foo").unwrap(),
                Some(Value::NULL)
            );
            conn.query_drop("SET @foo = 'foo'").unwrap();
            conn.reset_connection(false);
            drop(conn);

            conn = pool.get_conn().unwrap();
            assert_eq!(
                conn.query_first::<String, _>("SELECT @foo")
                    .unwrap()
                    .unwrap(),
                "foo",
            );
            drop(conn);

            let pool = Pool::new(opts.pool_opts(pool_opts.with_reset_connection(false))).unwrap();
            conn = pool.get_conn().unwrap();
            conn.query_drop("SET @foo = 'foo'").unwrap();
            drop(conn);
            conn = pool.get_conn().unwrap();
            assert_eq!(
                conn.query_first::<String, _>("SELECT @foo")
                    .unwrap()
                    .unwrap(),
                "foo",
            );
            drop(conn);
        }

        #[cfg(feature = "nightly")]
        mod bench {
            use test;

            use std::thread;

            use crate::{prelude::*, test_misc::get_opts, Pool};

            #[bench]
            fn many_prepexecs(bencher: &mut test::Bencher) {
                let pool = Pool::new(get_opts()).unwrap();
                bencher.iter(|| {
                    "SELECT 1".with(()).run(&pool).unwrap();
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
                                    "SELECT 1, 'hello world', 123.321, ?, ?, ?"
                                        .with(("hello", "world", 65536))
                                        .run(&pool)
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
                                    "SELECT 1, 'hello world', 123.321, ?, ?, ?"
                                        .with(("hello", "world", 65536))
                                        .run(&pool)
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
