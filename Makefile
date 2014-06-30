all: rust-mysql-simple doc

rust-mysql-simple:
	mkdir -p lib
	rustc --out-dir=lib --opt-level 3 src/lib.rs

doc:
	mkdir -p doc
	rustdoc -o doc src/lib.rs

test:
	rustc --test src/lib.rs --opt-level 3 -o mysql-test
	RUST_TEST_TASKS=1 ./mysql-test
	rm ./mysql-test

bench:
	rustc --test src/lib.rs --opt-level 3 -o mysql-test
	RUST_TEST_TASKS=1 ./mysql-test --bench
	rm ./mysql-test

clean:
	rm -rf lib
	rm -rf doc
