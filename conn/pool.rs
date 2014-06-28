use sync::{Arc, Mutex};
use super::super::error::{MyDriverError, InvalidPoolConstraints};
use super::{MyConn, MyOpts, Stmt, QueryResult};
use super::super::error::{MyResult};

struct MyInnerPool {
    opts: MyOpts,
    pool: Vec<MyConn>,
    min: uint,
    max: uint,
    count: uint
}

impl MyInnerPool {
    fn new(min: uint, max: uint, opts: MyOpts) -> MyResult<MyInnerPool> {
        if min > max || max == 0 {
            return Err(MyDriverError(InvalidPoolConstraints));
        }
        let mut pool = MyInnerPool {
            opts: opts,
            pool: Vec::with_capacity(max),
            max: max,
            min: min,
            count: 0
        };
        for _ in range(0, min) {
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

#[deriving(Clone)]
pub struct MyPool {
    pool: Arc<Mutex<MyInnerPool>>
}

impl MyPool {
    pub fn new(opts: MyOpts) -> MyResult<MyPool> {
        MyPool::new_manual(10, 100, opts)
    }

    pub fn new_manual(min: uint, max: uint, opts: MyOpts) -> MyResult<MyPool> {
        let pool = try!(MyInnerPool::new(min, max, opts));
        Ok(MyPool{ pool: Arc::new(Mutex::new(pool)) })
    }

    pub fn get_conn(&self) -> MyResult<MyPooledConn> {
        let mut pool = self.pool.lock();

        while pool.pool.is_empty() {
            if pool.count < pool.max {
                match pool.new_conn() {
                    Ok(()) => {
                        pool.count += 1;
                        break;
                    },
                    Err(err) => return Err(err)
                }
            } else {
                pool.cond.wait();
            }
        }

        let mut conn = pool.pool.pop().unwrap();

        if !conn.ping() {
            try!(conn.reset());
        }

        Ok(MyPooledConn {pool: self.clone(), conn: Some(conn)})
    }
    pub fn query(&self, query: &str) -> MyResult<QueryResult> {
        let conn = try!(self.get_conn());
        conn.pooled_query(query)
    }
    pub fn prepare(&self, query: &str) -> MyResult<Stmt> {
        let conn = try!(self.get_conn());
        conn.pooled_prepare(query)
    }
}

pub struct MyPooledConn {
    pool: MyPool,
    conn: Option<MyConn>
}

impl Drop for MyPooledConn {
    fn drop(&mut self) {
        let mut pool = self.pool.pool.lock();
        if pool.count > pool.min || self.conn.is_none() {
            pool.count -= 1;
        } else {
            pool.pool.push(self.conn.take_unwrap());
        }
        pool.cond.signal();
    }
}

impl MyPooledConn {
    pub fn query<'a>(&'a mut self, query: &str) -> MyResult<QueryResult<'a>> {
        self.conn.get_mut_ref().query(query)
    }
    pub fn prepare<'a>(&'a mut self, query: &str) -> MyResult<Stmt<'a>> {
        self.conn.get_mut_ref().prepare(query)
    }
    pub fn get_mut_ref<'a>(&'a mut self) -> &'a mut MyConn {
        self.conn.get_mut_ref()
    }
    pub fn get_ref<'a>(&'a self) -> &'a MyConn {
        self.conn.get_ref()
    }
    pub fn unwrap(mut self) -> MyConn {
        self.conn.take_unwrap()
    }
    fn pooled_query(mut self, query: &str) -> MyResult<QueryResult> {
        match self.get_mut_ref()._query(query) {
            Ok((columns, ok_packet)) => Ok(QueryResult::new_pooled(self, columns, ok_packet, false)),
            Err(err) => Err(err)
        }
    }
    fn pooled_prepare(mut self, query: &str) -> MyResult<Stmt> {
        match self.get_mut_ref()._prepare(query) {
            Ok(stmt) => Ok(Stmt::new_pooled(stmt, self)),
            Err(err) => Err(err)
        }
    }
}

#[cfg(test)]
mod test {
    use conn::{MyOpts};
    use std::default::{Default};
    use super::{MyPool};
    use super::super::super::value::{Bytes, Int};

    #[test]
    fn test_query() {
        let pool = MyPool::new(MyOpts{user: Some("root".to_string()),
                                      ..Default::default()});
        assert!(pool.is_ok());
        let pool = pool.unwrap();
        for _ in range(0u, 10u) {
            let pool = pool.clone();
            spawn(proc() {
                let conn = pool.get_conn();
                assert!(conn.is_ok());
                let mut conn = conn.unwrap();
                assert!(conn.query("SELECT 1").is_ok());
            });
        }
    }

    #[test]
    fn test_pooled_query() {
        let pool = MyPool::new(MyOpts{user: Some("root".to_string()),
                                      ..Default::default()});
        assert!(pool.is_ok());
        let pool = pool.unwrap();
        for _ in range(0u, 10u) {
            let pool = pool.clone();
            spawn(proc() {
                let result = pool.query("SELECT 1");
                assert!(result.is_ok());
                let mut result = result.unwrap();
                assert_eq!(result.next(), Some(Ok(vec![Bytes(vec![0x31u8])])));
            });
        }
    }

    #[test]
    fn test_prepared_query() {
        let pool = MyPool::new(MyOpts{user: Some("root".to_string()),
                                      ..Default::default()});
        assert!(pool.is_ok());
        let pool = pool.unwrap();
        for _ in range(0u, 10u) {
            let pool = pool.clone();
            spawn(proc() {
                let conn = pool.get_conn();
                assert!(conn.is_ok());
                let mut conn = conn.unwrap();
                let stmt = conn.prepare("SELECT 1");
                assert!(stmt.is_ok());
                let mut stmt = stmt.unwrap();
                assert!(stmt.execute([]).is_ok());
            });
        }
    }

    #[test]
    fn test_pooled_prepared_query() {
        let pool = MyPool::new(MyOpts{user: Some("root".to_string()),
                                      ..Default::default()});
        assert!(pool.is_ok());
        let pool = pool.unwrap();
        for _ in range(0u, 10u) {
            let pool = pool.clone();
            spawn(proc() {
                let stmt = pool.prepare("SELECT 1");
                assert!(stmt.is_ok());
                let mut stmt = stmt.unwrap();
                for _ in range(0u, 5u) {
                    let result = stmt.execute([]);
                    assert!(result.is_ok());
                    let mut result = result.unwrap();
                    assert_eq!(result.next(), Some(Ok(vec![Int(1)])));
                    assert_eq!(result.next(), None);
                }
            });
        }
    }
}
