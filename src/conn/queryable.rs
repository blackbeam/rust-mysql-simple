// Copyright (c) 2020 rust-mysql-simple contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use mysql_common::row::convert::FromRowError;

use std::{borrow::Cow, result::Result as StdResult};

use crate::{
    conn::query_result::{Binary, Text},
    from_row, from_row_opt,
    prelude::FromRow,
    Params, QueryResult, Result, Statement,
};

/// Something, that eventually is a `Statement` in the context of a `T: Queryable`.
pub trait AsStatement {
    /// Make a statement out of `Self`.
    fn as_statement<Q: Queryable>(&self, queryable: &mut Q) -> Result<Cow<'_, Statement>>;
}

/// Queryable object.
pub trait Queryable {
    /// Perfoms text query.
    fn query_iter<Q: AsRef<str>>(&mut self, query: Q) -> Result<QueryResult<'_, '_, '_, Text>>;

    /// Performs text query and collects the first result set.
    fn query<T, Q>(&mut self, query: Q) -> Result<Vec<T>>
    where
        Q: AsRef<str>,
        T: FromRow,
    {
        self.query_map(query, from_row)
    }

    /// Same as [`Queryable::query`] but useful when you not sure what your schema is.
    fn query_opt<T, Q>(&mut self, query: Q) -> Result<Vec<StdResult<T, FromRowError>>>
    where
        Q: AsRef<str>,
        T: FromRow,
    {
        self.query_map(query, from_row_opt)
    }

    /// Performs text query and returns the first row of the first result set.
    fn query_first<T, Q>(&mut self, query: Q) -> Result<Option<T>>
    where
        Q: AsRef<str>,
        T: FromRow,
    {
        self.query_iter(query)?
            .next()
            .map(|row| row.map(from_row))
            .transpose()
    }

    /// Same as [`Queryable::query_first`] but useful when you not sure what your schema is.
    fn query_first_opt<T, Q>(&mut self, query: Q) -> Result<Option<StdResult<T, FromRowError>>>
    where
        Q: AsRef<str>,
        T: FromRow,
    {
        self.query_iter(query)?
            .next()
            .map(|row| row.map(from_row_opt))
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

    /// Same as [`Queryable::query_map`] but useful when you not sure what your schema is.
    fn query_map_opt<T, F, Q, U>(&mut self, query: Q, mut f: F) -> Result<Vec<U>>
    where
        Q: AsRef<str>,
        T: FromRow,
        F: FnMut(StdResult<T, FromRowError>) -> U,
    {
        self.query_fold_opt(query, Vec::new(), |mut acc, row| {
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

    /// Same as [`Queryable::query_fold`] but useful when you not sure what your schema is.
    fn query_fold_opt<T, F, Q, U>(&mut self, query: Q, init: U, mut f: F) -> Result<U>
    where
        Q: AsRef<str>,
        T: FromRow,
        F: FnMut(U, StdResult<T, FromRowError>) -> U,
    {
        self.query_iter(query)?
            .map(|row| row.map(from_row_opt::<T>))
            .try_fold(init, |acc, row: Result<StdResult<T, FromRowError>>| {
                row.map(|row| f(acc, row))
            })
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

    /// Executes the given `stmt` with the given `params`.
    fn exec_iter<S, P>(&mut self, stmt: S, params: P) -> Result<QueryResult<'_, '_, '_, Binary>>
    where
        S: AsStatement,
        P: Into<Params>;

    /// Prepares the given statement, and executes it with each item in the given params iterator.
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

    /// Executes the given `stmt` and collects the first result set.
    fn exec<T, S, P>(&mut self, stmt: S, params: P) -> Result<Vec<T>>
    where
        S: AsStatement,
        P: Into<Params>,
        T: FromRow,
    {
        self.exec_map(stmt, params, from_row)
    }

    /// Same as [`Queryable::exec`] but useful when you not sure what your schema is.
    fn exec_opt<T, S, P>(&mut self, stmt: S, params: P) -> Result<Vec<StdResult<T, FromRowError>>>
    where
        S: AsStatement,
        P: Into<Params>,
        T: FromRow,
    {
        self.exec_map(stmt, params, from_row_opt)
    }

    /// Executes the given `stmt` and returns the first row of the first result set.
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

    /// Same as [`Queryable::exec_first`] but useful when you not sure what your schema is.
    fn exec_first_opt<T, S, P>(
        &mut self,
        stmt: S,
        params: P,
    ) -> Result<Option<StdResult<T, FromRowError>>>
    where
        S: AsStatement,
        P: Into<Params>,
        T: FromRow,
    {
        self.exec_iter(stmt, params)?
            .next()
            .map(|row| row.map(from_row_opt))
            .transpose()
    }

    /// Executes the given `stmt` and maps each row of the first result set.
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

    /// Same as [`Queryable::exec_map`] but useful when you not sure what your schema is.
    fn exec_map_opt<T, S, P, F, U>(&mut self, stmt: S, params: P, mut f: F) -> Result<Vec<U>>
    where
        S: AsStatement,
        P: Into<Params>,
        T: FromRow,
        F: FnMut(StdResult<T, FromRowError>) -> U,
    {
        self.exec_fold_opt(stmt, params, Vec::new(), |mut acc, row| {
            acc.push(f(row));
            acc
        })
    }

    /// Executes the given `stmt` and folds the first result set to a signel value.
    fn exec_fold<T, S, P, U, F>(&mut self, stmt: S, params: P, init: U, mut f: F) -> Result<U>
    where
        S: AsStatement,
        P: Into<Params>,
        T: FromRow,
        F: FnMut(U, T) -> U,
    {
        let mut result = self.exec_iter(stmt, params)?;
        result.try_fold(init, |init, row| row.map(|row| f(init, from_row(row))))
    }

    /// Same as [`Queryable::exec_fold`] but useful when you not sure what your schema is.
    fn exec_fold_opt<T, S, P, U, F>(&mut self, stmt: S, params: P, init: U, mut f: F) -> Result<U>
    where
        S: AsStatement,
        P: Into<Params>,
        T: FromRow,
        F: FnMut(U, StdResult<T, FromRowError>) -> U,
    {
        let mut result = self.exec_iter(stmt, params)?;
        result.try_fold(init, |init, row| row.map(|row| f(init, from_row_opt(row))))
    }

    /// Executes the given `stmt` and drops the result.
    fn exec_drop<S, P>(&mut self, stmt: S, params: P) -> Result<()>
    where
        S: AsStatement,
        P: Into<Params>,
    {
        self.exec_iter(stmt, params).map(drop)
    }
}
