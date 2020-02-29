// Copyright (c) 2020 rust-mysql-simple contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{convert::TryInto};

use crate::{conn::{queryable::AsStatement, ConnMut}, from_row, prelude::FromRow, Result, Error, QueryResult, Binary, Params, Text};

/// MySql text query.
///
/// This trait covers the set of `query*` methods on the `Queryable` trait.
/// Please see the corresponding section of the crate level docs for details.
///
/// Example:
///
/// ```rust
/// # mysql::doctest_wrapper!(__result, {
/// use mysql::*;
/// use mysql::prelude::*;
/// let pool = Pool::new(get_opts())?;
///
/// let num: Option<u32> = "SELECT 42".first(&pool)?;
///
/// assert_eq!(num, Some(42));
/// # });
/// ```
pub trait TextQuery: Sized {
    /// This methods corresponds to `Queryable::query_iter`.
    fn run<'a, 'b, 'c, C>(self, conn: C) -> Result<QueryResult<'a, 'b, 'c, Text>>
    where
        C: TryInto<ConnMut<'a, 'b, 'c>>,
        Error: From<<C as TryInto<ConnMut<'a, 'b, 'c>>>::Error>;

    /// This methods corresponds to `Queryable::query_first`.
    fn first<'a, 'b, 'c: 'b, T, C>(self, conn: C) -> Result<Option<T>>
    where
        C: TryInto<ConnMut<'a, 'b, 'c>>,
        Error: From<<C as TryInto<ConnMut<'a, 'b, 'c>>>::Error>,
        T: FromRow
    {
        self.run(conn)?.next().map(|row| row.map(from_row)).transpose()
    }

    /// This methods corresponds to `Queryable::query`.
    fn fetch<'a, 'b, 'c: 'b, T, C>(self, conn: C) -> Result<Vec<T>>
    where
        C: TryInto<ConnMut<'a, 'b, 'c>>,
        Error: From<<C as TryInto<ConnMut<'a, 'b, 'c>>>::Error>,
        T: FromRow,
    {
        self.run(conn)?.map(|rrow| rrow.map(from_row)).collect()
    }

    /// This methods corresponds to `Queryable::query_fold`.
    fn fold<'a, 'b, 'c: 'b, T, U, F, C>(self, conn: C, mut init: U, mut next: F) -> Result<U>
    where
        C: TryInto<ConnMut<'a, 'b, 'c>>,
        Error: From<<C as TryInto<ConnMut<'a, 'b, 'c>>>::Error>,
        T: FromRow,
        F: FnMut(U, T) -> U,
    {
        for row in self.run(conn)? {
            init = next(init, from_row(row?));
        }

        Ok(init)
    }

    /// This methods corresponds to `Queryable::query_map`.
    fn map<'a, 'b, 'c: 'b, T, U, F, C>(self, conn: C, mut map: F) -> Result<Vec<U>>
    where
        C: TryInto<ConnMut<'a, 'b, 'c>>,
        Error: From<<C as TryInto<ConnMut<'a, 'b, 'c>>>::Error>,
        T: FromRow,
        F: FnMut(T) -> U,
    {
        self.fold(conn, Vec::new(), |mut acc, row: T| {
            acc.push(map(row));
            acc
        })
    }
}

impl<Q: AsRef<str>> TextQuery for Q {
    fn run<'a, 'b, 'c, C>(self, conn: C) -> Result<QueryResult<'a, 'b, 'c, Text>>
    where
        C: TryInto<ConnMut<'a, 'b, 'c>>,
        Error: From<<C as TryInto<ConnMut<'a, 'b, 'c>>>::Error>,
    {
        let mut conn = conn.try_into()?;
        let meta = conn._query(self.as_ref())?;
        Ok(QueryResult::new(conn, meta))
    }
}

/// Representaion of a prepared statement query.
///
/// See `BinQuery` for details.
pub struct QueryWithParams<Q, P> {
    pub query: Q,
    pub params: P,
}

/// Helper, that constructs `QueryWithParams`.
pub trait WithParams: Sized {
    fn with<P>(self, params: P) -> QueryWithParams<Self, P>;
}

impl<T: AsRef<str>> WithParams for T {
    fn with<P>(self, params: P) -> QueryWithParams<Self, P> {
        QueryWithParams {
            query: self,
            params,
        }
    }
}

/// MySql prepared statement query.
///
/// This trait covers the set of `exec*` methods on the `Queryable` trait.
/// Please see the corresponding section of the crate level docs for details.
///
/// Example:
///
/// ```rust
/// # mysql::doctest_wrapper!(__result, {
/// use mysql::*;
/// use mysql::prelude::*;
/// let pool = Pool::new(get_opts())?;
///
/// let num: Option<u32> = "SELECT ?"
///     .with((42,))
///     .first(&pool)?;
///
/// assert_eq!(num, Some(42));
/// # });
/// ```
pub trait BinQuery: Sized {
    /// This methods corresponds to `Queryable::exec_iter`.
    fn run<'a, 'b, 'c, C>(self, conn: C) -> Result<QueryResult<'a, 'b, 'c, Binary>>
    where
        C: TryInto<ConnMut<'a, 'b, 'c>>,
        Error: From<<C as TryInto<ConnMut<'a, 'b, 'c>>>::Error>;

    /// This methods corresponds to `Queryable::exec_first`.
    fn first<'a, 'b, 'c: 'b, T, C>(self, conn: C) -> Result<Option<T>>
    where
        C: TryInto<ConnMut<'a, 'b, 'c>>,
        Error: From<<C as TryInto<ConnMut<'a, 'b, 'c>>>::Error>,
        T: FromRow
    {
        self.run(conn)?.next().map(|row| row.map(from_row)).transpose()
    }

    /// This methods corresponds to `Queryable::exec`.
    fn fetch<'a, 'b, 'c: 'b, T, C>(self, conn: C) -> Result<Vec<T>>
    where
        C: TryInto<ConnMut<'a, 'b, 'c>>,
        Error: From<<C as TryInto<ConnMut<'a, 'b, 'c>>>::Error>,
        T: FromRow
    {
        self.run(conn)?.map(|rrow| rrow.map(from_row)).collect()
    }

    /// This methods corresponds to `Queryable::exec_fold`.
    fn fold<'a, 'b, 'c: 'b, T, U, F, C>(self, conn: C, mut init: U, mut next: F) -> Result<U>
    where
        C: TryInto<ConnMut<'a, 'b, 'c>>,
        Error: From<<C as TryInto<ConnMut<'a, 'b, 'c>>>::Error>,
        T: FromRow,
        F: FnMut(U, T) -> U
    {
        for row in self.run(conn)? {
            init = next(init, from_row(row?));
        }

        Ok(init)
    }

    /// This methods corresponds to `Queryable::exec_map`.
    fn map<'a, 'b, 'c: 'b, T, U, F, C>(self, conn: C, mut map: F) -> Result<Vec<U>>
    where
        C: TryInto<ConnMut<'a, 'b, 'c>>,
        Error: From<<C as TryInto<ConnMut<'a, 'b, 'c>>>::Error>,
        T: FromRow,
        F: FnMut(T) -> U
    {
        self.fold(conn, Vec::new(), |mut acc, row: T| {
            acc.push(map(row));
            acc
        })
    }
}

impl<Q, P> BinQuery for QueryWithParams<Q, P>
where Q: AsStatement, P: Into<Params>
{
    fn run<'a, 'b, 'c, C>(self, conn: C) -> Result<QueryResult<'a, 'b, 'c, Binary>>
    where
        C: TryInto<ConnMut<'a, 'b, 'c>>,
        Error: From<<C as TryInto<ConnMut<'a, 'b, 'c>>>::Error>,
    {
        let mut conn = conn.try_into()?;
        let statement = self.query.as_statement(&mut *conn)?;
        let meta = conn._execute(&*statement, self.params.into())?;
        Ok(QueryResult::new(conn, meta))
    }
}

/// Helper trait for batch statement execution.
///
/// This trait covers the `Queryable::exec_batch` method.
/// Please see the corresponding section of the crate level docs for details.
///
/// Example:
///
/// ```rust
/// # mysql::doctest_wrapper!(__result, {
/// use mysql::*;
/// use mysql::prelude::*;
/// let pool = Pool::new(get_opts())?;
///
/// // This will prepare `DO ?` and execute `DO 0`, `DO 1`, `DO 2` and so on.
/// "DO ?"
///     .with((0..10).map(|x| (x,)))
///     .batch(&pool)?;
/// # });
/// ```
pub trait BatchQuery {
    fn batch<'a, 'b, 'c: 'b, C>(self, conn: C) -> Result<()>
    where
        C: TryInto<ConnMut<'a, 'b, 'c>>,
        Error: From<<C as TryInto<ConnMut<'a, 'b, 'c>>>::Error>;
}

impl<Q, I, P> BatchQuery for QueryWithParams<Q, I>
where Q: AsStatement, I: IntoIterator<Item=P>, P: Into<Params>
{
    /// This methods corresponds to `Queryable::exec_batch`.
    fn batch<'a, 'b, 'c: 'b, C>(self, conn: C) -> Result<()>
    where
        C: TryInto<ConnMut<'a, 'b, 'c>>,
        Error: From<<C as TryInto<ConnMut<'a, 'b, 'c>>>::Error>
    {
        let mut conn = conn.try_into()?;
        let statement = self.query.as_statement(&mut *conn)?;

        for params in self.params {
            let params = params.into();
            let meta = conn._execute(&*statement, params)?;
            let mut query_result = QueryResult::<Binary>::new((&mut *conn).into(), meta);
            while let Some(result_set) = query_result.next_set() {
                for row in result_set? {
                    row?;
                }
            }
        }

        Ok(())
    }
}