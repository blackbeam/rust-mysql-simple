// Copyright (c) 2020 rust-mysql-simple contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::borrow::Cow;

use crate::{from_row, prelude::FromRow, Params, QueryResult, Result, Statement};

pub trait AsStatement {
    fn as_statement<Q: Queryable>(&self, queryable: &mut Q) -> Result<Cow<'_, Statement>>;
}

/// Queryable object.
pub trait Queryable {
    fn query_iter<Q: AsRef<str>>(&mut self, query: Q) -> Result<QueryResult<'_>>;

    fn query<T, Q>(&mut self, query: Q) -> Result<Vec<T>>
    where
        Q: AsRef<str>,
        T: FromRow,
    {
        self.query_map(query, from_row)
    }

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

    fn query_drop<Q>(&mut self, query: Q) -> Result<()>
    where
        Q: AsRef<str>,
    {
        self.query_iter(query).map(drop)
    }

    fn prep<Q: AsRef<str>>(&mut self, query: Q) -> Result<crate::Statement>;

    /// This function will close the given statement on the server side.
    fn close(&mut self, stmt: Statement) -> Result<()>;

    fn exec_iter<S, P>(&mut self, stmt: S, params: P) -> Result<QueryResult<'_>>
    where
        S: AsStatement,
        P: Into<Params>;

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

    fn exec<T, S, P>(&mut self, stmt: S, params: P) -> Result<Vec<T>>
    where
        S: AsStatement,
        P: Into<Params>,
        T: FromRow,
    {
        self.exec_map(stmt, params, crate::from_row)
    }

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

    fn exec_drop<S, P>(&mut self, stmt: S, params: P) -> Result<()>
    where
        S: AsStatement,
        P: Into<Params>,
    {
        self.exec_iter(stmt, params).map(drop)
    }
}
