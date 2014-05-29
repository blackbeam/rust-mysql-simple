use sync::{Arc, Mutex};
use conn::{MyInnerConn, MyOpts, MyResult, Stmt, QueryResult};

struct MyInnerPool {
    opts: MyOpts,
    pool: Vec<MyInnerConn>,
    cap: uint,
    count: uint
}

impl MyInnerPool {
    fn new_conn(&mut self) -> MyResult<()> {
        match MyInnerConn::new(self.opts.clone()) {
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
    pub fn new(cap: uint, opts: MyOpts) -> MyPool {
        let pool = MyInnerPool {
            opts: opts,
            pool: Vec::new(),
            cap: cap,
            count: 0
        };
        MyPool{ pool: Arc::new(Mutex::new(pool)) }
    }

    pub fn get_conn(&self) -> MyResult<MyPooledConn> {
        let mut pool = self.pool.lock();

        while pool.pool.is_empty() {
            if pool.cap == 0 || pool.count < pool.cap {
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

        Ok(MyPooledConn {
            pool: self.clone(),
            conn: Some(pool.pool.pop().unwrap())
        })
    }
}

pub struct MyPooledConn {
    pool: MyPool,
    conn: Option<MyInnerConn>
}

#[unsafe_destructor]
impl Drop for MyPooledConn {
    fn drop(&mut self) {
        let mut pool = self.pool.pool.lock();
        pool.pool.push(self.conn.take_unwrap());
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
}

#[cfg(test)]
mod test {
    use conn::{MyOpts};
    use std::default::{Default};
    use super::{MyPool};

    #[test]
    fn test_query() {
        let pool = MyPool::new(3, MyOpts{user: Some("root".to_string()),
                                         ..Default::default()});
        for _ in range(0, 10) {
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
    fn test_prepared_query() {
        let pool = MyPool::new(3, MyOpts{user: Some("root".to_string()),
                                         ..Default::default()});
        for _ in range(0, 10) {
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
    fn test_zero_cap() {
        let pool = MyPool::new(0, MyOpts{user: Some("root".to_string()),
                                         ..Default::default()});
        for _ in range(0, 10) {
            let pool = pool.clone();
            spawn(proc() {
                let conn = pool.get_conn();
                assert!(conn.is_ok());
                let mut conn = conn.unwrap();
                assert!(conn.query("SELECT 1").is_ok());
            });
        }
    }
}
