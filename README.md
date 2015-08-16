rust-mysql-simple
=================
[![Build Status](https://travis-ci.org/blackbeam/rust-mysql-simple.png?branch=master)](https://travis-ci.org/blackbeam/rust-mysql-simple) [![Build status](https://ci.appveyor.com/api/projects/status/4te7c9q4tlmwvof0/branch/master?svg=true)](https://ci.appveyor.com/project/blackbeam/rust-mysql-simple/branch/master)

Mysql client library implemented in rust. Feel free to open a new issues and pull requests.

### Documentation
Latest crate API docs hosted [here](http://blackbeam.org/doc/mysql/index.html).

### Installation
Please use [crates.io](https://crates.io/crates/mysql)

```toml
[dependencies]
mysql = "*"
```

rust-mysql-simple offer support of SSL via `ssl` cargo feature which is enabled by default. If you have no plans to use SSL, then you should disable that feature to not to depend on rust-openssl:

```toml
[dependencies.mysql]
mysql = "*"
default-features = false
features = ["socket"]
```

### Windows support (since 0.18.0)
Currently rust-mysql-simple have no support of SSL nor named pipes. To use crate on Windows you have to disable default features.

```toml
[dependencies.mysql]
mysql = "*"
default-features = false
```

[Simple example](http://blackbeam.org/doc/mysql/index.html#example)
