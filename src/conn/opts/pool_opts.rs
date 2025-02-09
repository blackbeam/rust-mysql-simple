// Copyright (c) 2023 rust-mysql-simple contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

macro_rules! const_assert {
    ($name:ident, $($xs:expr),+ $(,)*) => {
        #[allow(unknown_lints, clippy::eq_op)]
        const $name: [(); 0 - !($($xs)&&+) as usize] = [];
    };
}

/// Connection pool options.
///
/// ```
/// # use mysql::{PoolOpts, PoolConstraints};
/// # use std::time::Duration;
/// let pool_opts = PoolOpts::default()
///     .with_constraints(PoolConstraints::new(15, 30).unwrap())
///     .with_reset_connection(false);
/// ```
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct PoolOpts {
    constraints: PoolConstraints,
    reset_connection: bool,
    check_health: bool,
}

impl PoolOpts {
    /// Calls `Self::default`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates the default [`PoolOpts`] with the given constraints.
    pub fn with_constraints(mut self, constraints: PoolConstraints) -> Self {
        self.constraints = constraints;
        self
    }

    /// Returns pool constraints.
    pub fn constraints(&self) -> PoolConstraints {
        self.constraints
    }

    /// Sets whether to reset the connection upon returning it to a pool (defaults to `true`).
    ///
    /// Default behavior increases reliability but comes with cons:
    ///
    /// * reset procedure removes all prepared statements, i.e. kills prepared statements cache
    /// * connection reset is quite fast but requires additional client-server roundtrip
    ///   (might require re-authentication for older servers)
    ///
    /// The purpose of the reset procedure is to:
    ///
    /// * rollback any opened transactions
    /// * reset transaction isolation level
    /// * reset session variables
    /// * delete user variables
    /// * remove temporary tables
    /// * remove all PREPARE statement (this action kills prepared statements cache)
    ///
    /// So to increase overall performance you can safely opt-out of the default behavior
    /// if you are not willing to change the session state in an unpleasant way.
    ///
    /// It is also possible to selectively opt-in/out using [`crate::PooledConn::reset_connection`].
    ///
    /// # Connection URL
    ///
    /// You can use `reset_connection` URL parameter to set this value. E.g.
    ///
    /// ```
    /// # use mysql::*;
    /// # use std::time::Duration;
    /// # fn main() -> Result<()> {
    /// let opts = Opts::from_url("mysql://localhost/db?reset_connection=false")?;
    /// assert_eq!(opts.get_pool_opts().reset_connection(), false);
    /// # Ok(()) }
    /// ```
    pub fn with_reset_connection(mut self, reset_connection: bool) -> Self {
        self.reset_connection = reset_connection;
        self
    }

    /// Returns the `reset_connection` value (see [`PoolOpts::with_reset_connection`]).
    pub fn reset_connection(&self) -> bool {
        self.reset_connection
    }

    /// Sets whether to check connection health upon retrieving it from a pool (defaults to `true`).
    ///
    /// If `true`, then `Conn::ping` will be invoked on a non-fresh pooled connection.
    ///
    /// # Connection URL
    ///
    /// Use `check_health` URL parameter to set this value. E.g.
    ///
    /// ```
    /// # use mysql::*;
    /// # use std::time::Duration;
    /// # fn main() -> Result<()> {
    /// let opts = Opts::from_url("mysql://localhost/db?check_health=false")?;
    /// assert_eq!(opts.get_pool_opts().check_health(), false);
    /// # Ok(()) }
    /// ```
    pub fn with_check_health(mut self, check_health: bool) -> Self {
        self.check_health = check_health;
        self
    }

    pub fn check_health(&self) -> bool {
        self.check_health
    }
}

impl Default for PoolOpts {
    fn default() -> Self {
        Self {
            constraints: PoolConstraints::DEFAULT,
            reset_connection: true,
            check_health: true,
        }
    }
}

/// Connection pool constraints.
///
/// This type stores `min` and `max` constraints for [`crate::Pool`] and ensures that `min <= max`.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct PoolConstraints {
    min: usize,
    max: usize,
}

const_assert!(
    _DEFAULT_POOL_CONSTRAINTS_ARE_CORRECT,
    PoolConstraints::DEFAULT.min <= PoolConstraints::DEFAULT.max,
);

const_assert!(
    _POOL_CONSTRAINTS_MIN_IS_NONZERO,
    PoolConstraints::DEFAULT.min > 0
);

const_assert!(
    _POOL_CONSTRAINTS_MAX_IS_NONZERO,
    PoolConstraints::DEFAULT.max > 0
);

pub struct Assert<const L: usize, const R: usize>;
impl<const L: usize, const R: usize> Assert<L, R> {
    pub const LEQ: usize = R - L;
}

#[allow(path_statements)]
pub const fn gte<const M: usize, const N: usize>() {
    #[allow(clippy::no_effect)]
    Assert::<M, N>::LEQ;
}

impl PoolConstraints {
    /// Default pool constraints.
    pub const DEFAULT: PoolConstraints = PoolConstraints { min: 10, max: 100 };

    /// Creates new [`PoolConstraints`] if constraints are valid (`min <= max`).
    ///
    /// # Connection URL
    ///
    /// You can use `pool_min` and `pool_max` URL parameters to define pool constraints.
    ///
    /// ```
    /// # use mysql::*;
    /// # fn main() -> Result<()> {
    /// let opts = Opts::from_url("mysql://localhost/db?pool_min=0&pool_max=151")?;
    /// assert_eq!(opts.get_pool_opts().constraints(), PoolConstraints::new(0, 151).unwrap());
    /// # Ok(()) }
    /// ```
    pub fn new(min: usize, max: usize) -> Option<PoolConstraints> {
        match (min, max) {
            (0, 0) => None,
            (min, max) if min <= max => Some(PoolConstraints { min, max }),
            _ => None,
        }
    }

    pub const fn new_const<const MIN: usize, const MAX: usize>() -> PoolConstraints {
        gte::<MIN, MAX>();

        assert!(MIN > 0);
        assert!(MAX > 0);

        PoolConstraints { min: MIN, max: MAX }
    }

    /// Lower bound of this pool constraints.
    pub const fn min(&self) -> usize {
        self.min
    }

    /// Upper bound of this pool constraints.
    pub const fn max(&self) -> usize {
        self.max
    }
}

impl Default for PoolConstraints {
    fn default() -> Self {
        PoolConstraints::DEFAULT
    }
}

impl From<PoolConstraints> for (usize, usize) {
    /// Transforms constraints to a pair of `(min, max)`.
    fn from(PoolConstraints { min, max }: PoolConstraints) -> Self {
        (min, max)
    }
}
