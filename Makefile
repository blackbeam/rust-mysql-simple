all: lib doc

target/deps: lib

target/tests/mysql: test

lib:
	cargo build

doc: target/deps
	mkdir -p doc
	rustdoc -o doc -L target/deps src/lib.rs

test:
	RUST_TEST_TASKS=1 cargo test

bench: target/tests/mysql
	RUST_TEST_TASKS=1 ./target/tests/mysql --bench

clean:
	rm -rf target
	rm -rf doc
