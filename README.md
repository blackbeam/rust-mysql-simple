rust-mysql-simple [![Build Status](https://travis-ci.org/blackbeam/rust-mysql-simple.png?branch=master)](https://travis-ci.org/blackbeam/rust-mysql-simple)
=================
Mysql client library implemented in rust. Feel free to open a new issues and pull requests.

### Documentation
Latest crate API docs hosted [here](http://blackbeam.org/doc/mysql/index.html).

### Installation
Please use [crates.io](https://crates.io/crates/mysql)

Also you can use git via another `[dependencies.*]` section in your Cargo.toml:

```toml
[dependencies.mysql]
git = "https://github.com/blackbeam/rust-mysql-simple"
```

rust-mysql-simple offer support of SSL via `ssl` cargo feature which is enabled by default. If you have no plans to use SSL, then you should disable that feature to not to depend on rust-openssl:

```toml
# For crates.io
[dependencies.mysql]
mysql = "*"
default-features = false

# For git
[dependencies.mysql]
git = "https://github.com/blackbeam/rust-mysql-simple"
default-features = false
```

[Simple example](http://blackbeam.org/doc/mysql/index.html#example)
