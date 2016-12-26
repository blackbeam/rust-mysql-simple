rust-mysql-simple
=================
[![Build Status](https://travis-ci.org/blackbeam/rust-mysql-simple.png?branch=master)](https://travis-ci.org/blackbeam/rust-mysql-simple) [![Build status](https://ci.appveyor.com/api/projects/status/4te7c9q4tlmwvof0/branch/master?svg=true)](https://ci.appveyor.com/project/blackbeam/rust-mysql-simple/branch/master)

Mysql client library implemented in rust. Feel free to open a new issues and pull requests.

### Changelog
Available [here](https://github.com/blackbeam/rust-mysql-simple/releases)

### Documentation
Latest crate API docs hosted [here](http://blackbeam.org/doc/mysql/index.html).

### Installation
Please use [crates.io](https://crates.io/crates/mysql)

```toml
[dependencies]
mysql = "*"
```

rust-mysql-simple offer support of SSL via `ssl` cargo feature which is disabled by default.
Add `ssl` feature to enable:

```toml
[dependencies.mysql]
version = "*"
features = ["ssl"]
```

### Windows support (since 0.18.0)
Windows is supported but currently rust-mysql-simple has no support of SSL on Windows.

[Simple example](http://blackbeam.org/doc/mysql/index.html#example)
