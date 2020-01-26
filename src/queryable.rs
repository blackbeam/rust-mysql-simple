use std::borrow::Cow;

use crate::{prelude::FromRow, Params, QueryResult, Result, Statement};

pub trait AsStatement {
    fn as_statement<Q: Queryable>(&self, queryable: &mut Q) -> Result<Cow<'_, Statement>>;
}

/// Queryable object.
pub trait Queryable {
    fn query_iter<T: AsRef<str>>(&mut self, query: T) -> Result<QueryResult<'_>>;

    fn query<T, U>(&mut self, query: T) -> Result<Vec<U>>
    where
        T: AsRef<str>,
        U: FromRow,
    {
        self.query_map(query, crate::from_row)
    }

    fn query_first<T, U>(&mut self, query: T) -> Result<Option<U>>
    where
        T: AsRef<str>,
        U: FromRow,
    {
        self.query_iter(query)?
            .next()
            .map(|row| row.map(crate::from_row))
            .transpose()
    }

    fn query_map<F, T, U>(&mut self, query: T, mut f: F) -> Result<Vec<U>>
    where
        T: AsRef<str>,
        F: FnMut(crate::Row) -> U,
    {
        self.query_fold(query, Vec::new(), |mut acc, row| {
            acc.push(f(row));
            acc
        })
    }

    fn query_fold<F, T, U>(&mut self, query: T, init: U, mut f: F) -> Result<U>
    where
        T: AsRef<str>,
        F: FnMut(U, crate::Row) -> U,
    {
        self.query_iter(query)?
            .try_fold(init, |acc, row| row.map(|row| f(acc, row)))
    }

    fn query_drop<T>(&mut self, query: T) -> Result<()>
    where
        T: AsRef<str>,
    {
        self.query_iter(query).map(drop)
    }

    /// Implements binary protocol of mysql server.
    ///
    /// Prepares mysql statement on `Conn`. [`Stmt`](struct.Stmt.html) will
    /// borrow `Conn` until the end of its scope.
    ///
    /// This call will take statement from cache if has been prepared on this connection.
    ///
    /// ### JSON caveats
    ///
    /// For the following statement you will get somewhat unexpected result `{"a": 0}`, because
    /// booleans in mysql binary protocol is `TINYINT(1)` and will be interpreted as `0`:
    ///
    /// ```ignore
    /// pool.prep_exec(r#"SELECT JSON_REPLACE('{"a": true}', '$.a', ?)"#, (false,));
    /// ```
    ///
    /// You should wrap such parameters to a proper json value. For example:
    ///
    /// ```ignore
    /// pool.prep_exec(r#"SELECT JSON_REPLACE('{"a": true}', '$.a', ?)"#, (Value::Bool(false),));
    /// ```
    ///
    /// ### Named parameters support
    ///
    /// `prepare` supports named parameters in form of `:named_param_name`. Allowed characters for
    /// parameter name are `[a-z_]`. Named parameters will be converted to positional before actual
    /// call to prepare so `SELECT :a-:b, :a*:b` is actually `SELECT ?-?, ?*?`.
    ///
    /// ```
    /// # #[macro_use] extern crate mysql; fn main() {
    /// # use mysql::prelude::*;
    /// # use mysql::{Pool, Opts, OptsBuilder, from_row};
    /// # use mysql::Error::DriverError;
    /// # use mysql::DriverError::MixedParams;
    /// # use mysql::DriverError::MissingNamedParameter;
    /// # use mysql::DriverError::NamedParamsForPositionalQuery;
    /// # fn get_opts() -> Opts {
    /// #     let url = if let Ok(url) = std::env::var("DATABASE_URL") {
    /// #         let opts = Opts::from_url(&url).expect("DATABASE_URL invalid");
    /// #         if opts.get_db_name().expect("a database name is required").is_empty() {
    /// #             panic!("database name is empty");
    /// #         }
    /// #         url
    /// #     } else {
    /// #         "mysql://root:password@127.0.0.1:3307/mysql".to_string()
    /// #     };
    /// #     Opts::from_url(&*url).unwrap()
    /// # }
    /// # let opts = get_opts();
    /// # let pool = Pool::new(opts).unwrap();
    /// # let mut conn = pool.get_conn().unwrap();
    /// // Names could be repeated
    /// conn.exec_iter("SELECT :a+:b, :a * :b, ':c'", params!{"a" => 2, "b" => 3}).map(|mut result| {
    ///     let row = result.next().unwrap().unwrap();
    ///     assert_eq!((5, 6, String::from(":c")), from_row(row));
    /// }).unwrap();
    ///
    /// // You can call named statement with positional parameters
    /// conn.exec_iter("SELECT :a+:b, :a*:b", (2, 3, 2, 3)).map(|mut result| {
    ///     let row = result.next().unwrap().unwrap();
    ///     assert_eq!((5, 6), from_row(row));
    /// }).unwrap();
    ///
    /// // You must pass all named parameters for statement
    /// let err = conn.exec_drop("SELECT :name", params!{"another_name" => 42}).unwrap_err();
    /// match err {
    ///     DriverError(e) => {
    ///         assert_eq!(MissingNamedParameter("name".into()), e);
    ///     }
    ///     _ => unreachable!(),
    /// }
    ///
    /// // You can't call positional statement with named parameters
    /// let err = conn.exec_drop("SELECT ?", params!{"first" => 42}).unwrap_err();
    /// match err {
    ///     DriverError(e) => assert_eq!(NamedParamsForPositionalQuery, e),
    ///     _ => unreachable!(),
    /// }
    ///
    /// // You can't mix named and positional parameters
    /// let err = conn.prep("SELECT :a, ?").unwrap_err();
    /// match err {
    ///     DriverError(e) => assert_eq!(MixedParams, e),
    ///     _ => unreachable!(),
    /// }
    /// # }
    /// ```
    fn prep<Q: AsRef<str>>(&mut self, query: Q) -> Result<crate::Statement>;

    /// This function will close the given statement on the server side.
    fn close(&mut self, stmt: Statement) -> Result<()>;

    fn exec_iter<S, P>(&mut self, stmt: S, params: P) -> Result<QueryResult<'_>>
    where
        S: AsStatement,
        P: Into<Params>;

    fn exec<S, P, T>(&mut self, stmt: S, params: P) -> Result<Vec<T>>
    where
        S: AsStatement,
        P: Into<Params>,
        T: FromRow,
    {
        self.exec_map(stmt, params, crate::from_row)
    }

    fn exec_first<S, P, T>(&mut self, stmt: S, params: P) -> Result<Option<T>>
    where
        S: AsStatement,
        P: Into<Params>,
        T: FromRow,
    {
        self.exec_iter(stmt, params)?
            .next()
            .map(|row| row.map(crate::from_row))
            .transpose()
    }

    fn exec_map<S, P, F, T>(&mut self, stmt: S, params: P, mut f: F) -> Result<Vec<T>>
    where
        S: AsStatement,
        P: Into<Params>,
        F: FnMut(crate::Row) -> T,
    {
        self.exec_fold(stmt, params, Vec::new(), |mut acc, row| {
            acc.push(f(row));
            acc
        })
    }

    fn exec_fold<S, P, T, F>(&mut self, stmt: S, params: P, init: T, mut f: F) -> Result<T>
    where
        S: AsStatement,
        P: Into<Params>,
        F: FnMut(T, crate::Row) -> T,
    {
        let mut result = self.exec_iter(stmt, params)?;
        let output = result.try_fold(init, |init, row| row.map(|row| f(init, row)));
        output
    }

    fn exec_drop<S, P>(&mut self, stmt: S, params: P) -> Result<()>
    where
        S: AsStatement,
        P: Into<Params>,
    {
        self.exec_iter(stmt, params).map(drop)
    }
}
