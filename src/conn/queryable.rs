// Copyright (c) 2020 rust-mysql-simple contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::borrow::Cow;

use crate::{
    conn::query_result::{Binary, Text}, from_row, prelude::FromRow, Params, QueryResult, Result,
    Statement
};

/// Something, that eventualy is a `Statement` in the context of a `T: Queryable`.
pub trait AsStatement {
    /// Make a statement out of `Self`.
    fn as_statement<Q: Queryable>(&self, queryable: &mut Q) -> Result<Cow<'_, Statement>>;
}

/// Queryable object.
pub trait Queryable {
    /// Perfoms text query.
    fn query_iter<Q: AsRef<str>>(&mut self, query: Q) -> Result<QueryResult<'_, '_, '_, Text>>;

    /// Performst text query and collects the first result set.
    fn query<T, Q>(&mut self, query: Q) -> Result<Vec<T>>
    where
        Q: AsRef<str>,
        T: FromRow,
    {
        self.query_map(query, from_row)
    }

    /// Performs text query and returns the firt row of the first result set.
    fn query_first<T, Q>(&mut self, query: Q) -> Result<Option<T>>
    where
        Q: AsRef<str>,
        T: FromRow,
    {
        self.query_iter(query)?
            .next()
            .map(|row| row.map(crate::from_row))
            .transpose()
    }

    /// Performs text query and maps each row of the first result set.
    fn query_map<T, F, Q, U>(&mut self, query: Q, mut f: F) -> Result<Vec<U>>
    where
        Q: AsRef<str>,
        T: FromRow,
        F: FnMut(T) -> U,
    {
        self.query_fold(query, Vec::new(), |mut acc, row| {
            acc.push(f(row));
            acc
        })
    }

    /// Performs text query and folds the first result set to a single value.
    fn query_fold<T, F, Q, U>(&mut self, query: Q, init: U, mut f: F) -> Result<U>
    where
        Q: AsRef<str>,
        T: FromRow,
        F: FnMut(U, T) -> U,
    {
        self.query_iter(query)?
            .map(|row| row.map(from_row::<T>))
            .try_fold(init, |acc, row: Result<T>| row.map(|row| f(acc, row)))
    }

    /// Performs text query and drops the query result.
    fn query_drop<Q>(&mut self, query: Q) -> Result<()>
    where
        Q: AsRef<str>,
    {
        self.query_iter(query).map(drop)
    }

    /// Prepares the given `query` as a prepared statement.
    fn prep<Q: AsRef<str>>(&mut self, query: Q) -> Result<crate::Statement>;

    /// This function will close the given statement on the server side.
    fn close(&mut self, stmt: Statement) -> Result<()>;

    /// Exectues the given `stmt` with the given `params`.
    fn exec_iter<S, P>(&mut self, stmt: S, params: P) -> Result<QueryResult<'_, '_, '_, Binary>>
    where
        S: AsStatement,
        P: Into<Params>;

    /// Prepares the given statement, and exectues it with each item in the given params iterator.
    fn exec_batch<S, P, I>(&mut self, stmt: S, params: I) -> Result<()>
    where
        Self: Sized,
        S: AsStatement,
        P: Into<Params>,
        I: IntoIterator<Item = P>,
    {
        let stmt = stmt.as_statement(self)?;
        for params in params {
            self.exec_drop(stmt.as_ref(), params)?;
        }

        Ok(())
    }

    /// Exectues the given `stmt` and collects the first result set.
    fn exec<T, S, P>(&mut self, stmt: S, params: P) -> Result<Vec<T>>
    where
        S: AsStatement,
        P: Into<Params>,
        T: FromRow,
    {
        self.exec_map(stmt, params, crate::from_row)
    }

    /// Exectues the given `stmt` and returns the first row of the first result set.
    fn exec_first<T, S, P>(&mut self, stmt: S, params: P) -> Result<Option<T>>
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

    /// Exectues the given `stmt` and maps each row of the first result set.
    fn exec_map<T, S, P, F, U>(&mut self, stmt: S, params: P, mut f: F) -> Result<Vec<U>>
    where
        S: AsStatement,
        P: Into<Params>,
        T: FromRow,
        F: FnMut(T) -> U,
    {
        self.exec_fold(stmt, params, Vec::new(), |mut acc, row| {
            acc.push(f(row));
            acc
        })
    }

    /// Exectues the given `stmt` and folds the first result set to a signel value.
    fn exec_fold<T, S, P, U, F>(&mut self, stmt: S, params: P, init: U, mut f: F) -> Result<U>
    where
        S: AsStatement,
        P: Into<Params>,
        T: FromRow,
        F: FnMut(U, T) -> U,
    {
        let mut result = self.exec_iter(stmt, params)?;
        let output = result.try_fold(init, |init, row| row.map(|row| f(init, from_row(row))));
        output
    }

    /// Exectues the given `stmt` and drops the result.
    fn exec_drop<S, P>(&mut self, stmt: S, params: P) -> Result<()>
    where
        S: AsStatement,
        P: Into<Params>,
    {
        self.exec_iter(stmt, params).map(drop)
    }
}
