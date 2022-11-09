// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

mod disabled;
mod enabled;

#[cfg(feature = "buffer-pool")]
pub use enabled::{get_buffer, Buffer};

#[cfg(not(feature = "buffer-pool"))]
pub use disabled::{get_buffer, Buffer};
