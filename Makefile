mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
mkfile_dir := $(dir $(mkfile_path))
MYSQL_DATA_DIR = $(mkfile_dir)tests/rust-mysql-simple-test
MYSQL_SSL_CA = $(mkfile_dir)tests/ca-cert.pem
MYSQL_SSL_CERT = $(mkfile_dir)tests/server-cert.pem
MYSQL_SSL_KEY = $(mkfile_dir)tests/server-key.pem
MYSQL_PORT = 3307
BASEDIR := $(shell mysqld --verbose --help 2>/dev/null | grep -e '^basedir' | awk '{ print $$2 }')

all: lib doc

target/deps: lib

target/tests/mysql: test

lib:
	cargo build --release

doc:
	cargo doc

test:
	bash -c "if [ -e $(MYSQL_DATA_DIR)/mysqld.pid ]; \
			 then \
				 kill -9 `cat $(MYSQL_DATA_DIR)/mysqld.pid`;\
				 rm -rf $(MYSQL_DATA_DIR);\
			 fi"

	bash -c "if [ -e $(MYSQL_DATA_DIR) ];\
			 then\
				 rm -rf $(MYSQL_DATA_DIR);\
			 fi"

	mkdir $(MYSQL_DATA_DIR)

	mysql_install_db \
		--no-defaults \
		--basedir=$(BASEDIR) \
		--datadir=$(MYSQL_DATA_DIR) \
		--force

	mysqld \
		--no-defaults \
		--basedir=$(BASEDIR) \
		--bind-address=127.0.0.1 \
		--datadir=$(MYSQL_DATA_DIR) \
		--max-allowed-packet=32M \
		--pid-file=$(MYSQL_DATA_DIR)/mysqld.pid \
		--port=$(MYSQL_PORT) \
		--innodb_file_per_table=1 \
		--innodb_file_format=Barracuda \
		--innodb_log_file_size=256M \
		--ssl \
		--ssl-ca=$(MYSQL_SSL_CA) \
		--ssl-cert=$(MYSQL_SSL_CERT) \
		--ssl-key=$(MYSQL_SSL_KEY) \
		--ssl-cipher=DHE-RSA-AES256-SHA \
		--socket=$(MYSQL_DATA_DIR)/mysqld.sock &
	sleep 10
	mysqladmin -h127.0.0.1 --port=$(MYSQL_PORT) -u root password 'password'

	bash -c "\
		if (RUST_TEST_TASKS=1 cargo test);\
		then \
			exit 0;\
		else\
			kill -9 `cat $(MYSQL_DATA_DIR)/mysqld.pid`;\
			rm -rf $(MYSQL_DATA_DIR);\
			exit 1;\
		fi"

	bash -c "\
		if (RUST_TEST_TASKS=1 cargo test --no-default-features);\
		then \
			exit 0;\
		else\
			kill -9 `cat $(MYSQL_DATA_DIR)/mysqld.pid`;\
			rm -rf $(MYSQL_DATA_DIR);\
			exit 1;\
		fi"

	kill -9 `cat $(MYSQL_DATA_DIR)/mysqld.pid`
	rm -rf $(MYSQL_DATA_DIR)

bench:
	bash -c "if [ -e $(MYSQL_DATA_DIR)/mysqld.pid ]; \
			 then \
				 kill -9 `cat $(MYSQL_DATA_DIR)/mysqld.pid`;\
				 rm -rf $(MYSQL_DATA_DIR);\
			 fi"

	bash -c "if [ -e $(MYSQL_DATA_DIR) ];\
			 then\
				 rm -rf $(MYSQL_DATA_DIR);\
			 fi"

	mkdir $(MYSQL_DATA_DIR)

	mysql_install_db \
		--no-defaults \
		--basedir=$(BASEDIR) \
		--datadir=$(MYSQL_DATA_DIR) \
		--force

	mysqld \
		--no-defaults \
		--basedir=$(BASEDIR) \
		--bind-address=127.0.0.1 \
		--datadir=$(MYSQL_DATA_DIR) \
		--max-allowed-packet=32M \
		--pid-file=$(MYSQL_DATA_DIR)/mysqld.pid \
		--port=$(MYSQL_PORT) \
		--innodb_file_per_table=1 \
		--innodb_file_format=Barracuda \
		--innodb_log_file_size=256M \
		--ssl \
		--ssl-ca=$(MYSQL_SSL_CA) \
		--ssl-cert=$(MYSQL_SSL_CERT) \
		--ssl-key=$(MYSQL_SSL_KEY) \
		--ssl-cipher=DHE-RSA-AES256-SHA \
		--socket=$(MYSQL_DATA_DIR)/mysqld.sock &
	sleep 10
	mysqladmin -h127.0.0.1 --port=$(MYSQL_PORT) -u root password 'password'

	bash -c "\
		if (RUST_TEST_TASKS=1 cargo bench);\
		then \
			exit 0;\
		else\
			kill -9 `cat $(MYSQL_DATA_DIR)/mysqld.pid`;\
			rm -rf $(MYSQL_DATA_DIR);\
			exit 1;\
		fi"

	bash -c "\
		if (RUST_TEST_TASKS=1 cargo bench --no-default-features);\
		then \
			exit 0;\
		else\
			kill -9 `cat $(MYSQL_DATA_DIR)/mysqld.pid`;\
			rm -rf $(MYSQL_DATA_DIR);\
			exit 1;\
		fi"

	kill -9 `cat $(MYSQL_DATA_DIR)/mysqld.pid`
	rm -rf $(MYSQL_DATA_DIR)
	

clean:
	cargo clean
