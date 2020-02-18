use mysql_common::{named_params::parse_named_params, packets::ComStmtClose};

use std::{
    borrow::{Borrow, Cow},
    convert::{Infallible, TryFrom, TryInto},
    marker::PhantomData,
};

use crate::{
    from_row, prelude::FromRow, Binary, Conn, Error, Params, Pool, PooledConn, QueryResponse,
    Result, Row, Statement, Text, Transaction,
};

pub trait AsStatement {
    fn as_statement(&self, queryable: &mut Conn) -> Result<Cow<'_, Statement>>;
}

#[derive(Debug)]
pub enum ConnMut<'a> {
    Mut(&'a mut Conn),
    Owned(Conn),
    Pooled(PooledConn),
}

impl From<Conn> for ConnMut<'static> {
    fn from(conn: Conn) -> Self {
        ConnMut::Owned(conn)
    }
}

impl From<PooledConn> for ConnMut<'static> {
    fn from(conn: PooledConn) -> Self {
        ConnMut::Pooled(conn)
    }
}

impl<'a> From<&'a mut Conn> for ConnMut<'a> {
    fn from(conn: &'a mut Conn) -> Self {
        ConnMut::Mut(conn)
    }
}

impl<'a> From<&'a mut PooledConn> for ConnMut<'a> {
    fn from(conn: &'a mut PooledConn) -> Self {
        ConnMut::Mut(conn.as_mut())
    }
}

impl<'a> TryFrom<&Pool> for ConnMut<'static> {
    type Error = Error;

    fn try_from(pool: &Pool) -> Result<Self> {
        pool.get_conn().map(From::from)
    }
}

impl<'a> std::ops::Deref for ConnMut<'a> {
    type Target = Conn;

    fn deref(&self) -> &Conn {
        match self {
            ConnMut::Mut(conn) => &**conn,
            ConnMut::Owned(conn) => &conn,
            ConnMut::Pooled(conn) => conn.as_ref(),
        }
    }
}

impl<'a> std::ops::DerefMut for ConnMut<'a> {
    fn deref_mut(&mut self) -> &mut Conn {
        match self {
            ConnMut::Mut(ref mut conn) => &mut **conn,
            ConnMut::Owned(ref mut conn) => conn,
            ConnMut::Pooled(ref mut conn) => conn.as_mut(),
        }
    }
}

pub trait Connection {
    fn as_mut(&mut self) -> &mut Conn;
}

/// Queryable object.
pub trait Queryable {
    /// Performs the given query.
    fn query<'a, T>(self, query: T) -> Result<QueryResponse<'a, Text>>
    where
        T: AsRef<str>,
        Self: TryInto<ConnMut<'a>>,
        Error: From<<Self as TryInto<ConnMut<'a>>>::Error>;

    /// Prepares the given statement.
    fn prep<'a, Q: AsRef<str>>(self, query: Q) -> Result<Statement>
    where
        Self: TryInto<ConnMut<'a>>,
        Error: From<<Self as TryInto<ConnMut<'a>>>::Error>;

    /// Closes the given statement.
    fn close<'a>(self, stmt: Statement) -> Result<()>
    where
        Self: TryInto<ConnMut<'a>>,
        Error: From<<Self as TryInto<ConnMut<'a>>>::Error>;

    fn exec<'a, S, P>(self, stmt: S, params: P) -> Result<QueryResponse<'a, Binary>>
    where
        S: AsStatement,
        P: Into<Params>,
        Self: TryInto<ConnMut<'a>>,
        Error: From<<Self as TryInto<ConnMut<'a>>>::Error>;
}

impl<Q: AsRef<str>> TextQuery for Q {
    fn run<'b, C>(&self, conn: C) -> Result<QueryResponse<'b, Text>>
    where
        C: TryInto<ConnMut<'b>>,
        Error: From<<C as TryInto<ConnMut<'b>>>::Error>,
    {
        conn.try_into()?.query(self.as_ref())
    }

    fn first<'b, T, C>(&self, conn: C) -> Result<Option<T>>
    where
        C: TryInto<ConnMut<'b>>,
        Error: From<<C as TryInto<ConnMut<'b>>>::Error>,
        T: FromRow,
    {
        Ok(self.run(conn)?.next().transpose()?.map(from_row))
    }

    fn fetch<'b, T, C>(&self, conn: C) -> Result<Vec<T>>
    where
        C: TryInto<ConnMut<'b>>,
        Error: From<<C as TryInto<ConnMut<'b>>>::Error>,
        T: FromRow,
    {
        self.run(conn)?.map(|rrow| rrow.map(from_row)).collect()
    }

    fn fold<'b, T, U, F, C>(&self, conn: C, mut init: U, mut next: F) -> Result<U>
    where
        C: TryInto<ConnMut<'b>>,
        Error: From<<C as TryInto<ConnMut<'b>>>::Error>,
        T: FromRow,
        F: FnMut(U, T) -> U,
    {
        for row in self.run(conn)? {
            init = next(init, from_row(row?));
        }

        Ok(init)
    }

    fn map<'b, T, U, F, C>(&self, conn: C, mut map: F) -> Result<Vec<U>>
    where
        C: TryInto<ConnMut<'b>>,
        Error: From<<C as TryInto<ConnMut<'b>>>::Error>,
        T: FromRow,
        F: FnMut(T) -> U,
    {
        self.fold(conn, Vec::new(), |mut acc, row: T| {
            acc.push(map(row));
            acc
        })
    }
}

pub trait TextQuery {
    fn run<'a, C>(&self, conn: C) -> Result<QueryResponse<'a, Text>>
    where
        C: TryInto<ConnMut<'a>>,
        Error: From<<C as TryInto<ConnMut<'a>>>::Error>;

    fn first<'a, T, C>(&self, conn: C) -> Result<Option<T>>
    where
        C: TryInto<ConnMut<'a>>,
        Error: From<<C as TryInto<ConnMut<'a>>>::Error>,
        T: FromRow;

    fn fetch<'a, T, C>(&self, conn: C) -> Result<Vec<T>>
    where
        C: TryInto<ConnMut<'a>>,
        Error: From<<C as TryInto<ConnMut<'a>>>::Error>,
        T: FromRow,
    {
        self.run(conn)?.map(|rrow| rrow.map(from_row)).collect()
    }

    fn fold<'a, T, U, F, C>(&self, conn: C, mut init: U, mut next: F) -> Result<U>
    where
        C: TryInto<ConnMut<'a>>,
        Error: From<<C as TryInto<ConnMut<'a>>>::Error>,
        T: FromRow,
        F: FnMut(U, T) -> U,
    {
        for row in self.run(conn)? {
            init = next(init, from_row(row?));
        }

        Ok(init)
    }

    fn map<'a, T, U, F, C>(&self, conn: C, mut map: F) -> Result<Vec<U>>
    where
        C: TryInto<ConnMut<'a>>,
        Error: From<<C as TryInto<ConnMut<'a>>>::Error>,
        T: FromRow,
        F: FnMut(T) -> U,
    {
        self.fold(conn, Vec::new(), |mut acc, row: T| {
            acc.push(map(row));
            acc
        })
    }
}

impl<Q: AsStatement> BinQuery for Q {
    fn s_run<'a, C, P>(&self, conn: C, params: P) -> Result<QueryResponse<'a, Binary>>
    where
        C: TryInto<ConnMut<'a>>,
        Error: From<<C as TryInto<ConnMut<'a>>>::Error>,
        P: Into<Params>,
    {
        let mut conn = conn.try_into()?;
        let statement = self.as_statement(&mut *conn)?;
        conn.exec(statement.as_ref(), params)
    }

    fn s_first<'a, T, C, P>(&self, conn: C, params: P) -> Result<Option<T>>
    where
        C: TryInto<ConnMut<'a>>,
        Error: From<<C as TryInto<ConnMut<'a>>>::Error>,
        P: Into<Params>,
        T: FromRow,
    {
        Ok(self.s_run(conn, params)?.next().transpose()?.map(from_row))
    }

    fn s_fetch<'a, T, C, P>(&self, conn: C, params: P) -> Result<Vec<T>>
    where
        C: TryInto<ConnMut<'a>>,
        Error: From<<C as TryInto<ConnMut<'a>>>::Error>,
        P: Into<Params>,
        T: FromRow,
    {
        self.s_run(conn, params)?
            .map(|row| row.map(from_row))
            .collect()
    }

    fn batch<'a, C, P, I>(&self, conn: C, params: I) -> Result<()>
    where
        C: TryInto<ConnMut<'a>>,
        Error: From<<C as TryInto<ConnMut<'a>>>::Error>,
        I: IntoIterator<Item = P>,
        P: Into<Params>,
    {
        let mut conn = conn.try_into()?;
        let statement = self.as_statement(&mut *conn)?;
        for params in params {
            (&mut *conn).exec(statement.as_ref(), params)?;
        }

        Ok(())
    }

    fn s_fold<'a, T, U, F, C, P>(&self, conn: C, params: P, mut init: U, mut next: F) -> Result<U>
    where
        C: TryInto<ConnMut<'a>>,
        Error: From<<C as TryInto<ConnMut<'a>>>::Error>,
        T: FromRow,
        F: FnMut(U, T) -> U,
        P: Into<Params>,
    {
        for row in self.s_run(conn, params)? {
            init = next(init, from_row(row?));
        }

        Ok(init)
    }

    fn s_map<'a, T, U, F, C, P>(&self, conn: C, params: P, mut map: F) -> Result<Vec<U>>
    where
        C: TryInto<ConnMut<'a>>,
        Error: From<<C as TryInto<ConnMut<'a>>>::Error>,
        T: FromRow,
        F: FnMut(T) -> U,
        P: Into<Params>,
    {
        self.s_fold(conn, params, Vec::new(), |mut acc, row: T| {
            acc.push(map(row));
            acc
        })
    }
}

pub trait BinQuery {
    fn s_run<'a, C, P>(&self, conn: C, params: P) -> Result<QueryResponse<'a, Binary>>
    where
        C: TryInto<ConnMut<'a>>,
        Error: From<<C as TryInto<ConnMut<'a>>>::Error>,
        P: Into<Params>;

    fn s_first<'a, T, C, P>(&self, conn: C, params: P) -> Result<Option<T>>
    where
        C: TryInto<ConnMut<'a>>,
        Error: From<<C as TryInto<ConnMut<'a>>>::Error>,
        P: Into<Params>,
        T: FromRow;

    fn s_fetch<'a, T, C, P>(&self, conn: C, params: P) -> Result<Vec<T>>
    where
        C: TryInto<ConnMut<'a>>,
        Error: From<<C as TryInto<ConnMut<'a>>>::Error>,
        P: Into<Params>,
        T: FromRow;

    fn batch<'a, C, P, I>(&self, conn: C, params: I) -> Result<()>
    where
        C: TryInto<ConnMut<'a>>,
        Error: From<<C as TryInto<ConnMut<'a>>>::Error>,
        I: IntoIterator<Item = P>,
        P: Into<Params>;

    fn s_fold<'a, T, U, F, C, P>(&self, conn: C, params: P, mut init: U, mut next: F) -> Result<U>
    where
        C: TryInto<ConnMut<'a>>,
        Error: From<<C as TryInto<ConnMut<'a>>>::Error>,
        T: FromRow,
        F: FnMut(U, T) -> U,
        P: Into<Params>;

    fn s_map<'a, T, U, F, C, P>(&self, conn: C, params: P, mut map: F) -> Result<Vec<U>>
    where
        C: TryInto<ConnMut<'a>>,
        Error: From<<C as TryInto<ConnMut<'a>>>::Error>,
        T: FromRow,
        F: FnMut(T) -> U,
        P: Into<Params>;
}
