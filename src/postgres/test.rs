extern mod postgres;

#[test]
fn test_conn() {
	printfln!("%?", postgres::open("postgres://postgres@localhost"));
}
