all: rust-mysql-simple doc

rust-mysql-simple:
	mkdir -p lib
	rustc --out-dir=lib --opt-level 3 lib.rs

doc:
	mkdir -p doc
	rustdoc -o doc lib.rs

test:
	rustc --test lib.rs --opt-level 3
	RUST_TEST_TASKS=1 ./rust-mysql-simple
	rm ./rust-mysql-simple

bench:
	rustc --test lib.rs --opt-level 3
	RUST_TEST_TASKS=1 ./rust-mysql-simple --bench
	rm ./rust-mysql-simple

clean:
	rm -rf lib
	rm -rf doc
