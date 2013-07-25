extern mod sqlite3;

#[test]
fn test() {
    let conn = sqlite3::open("db").unwrap();
    conn.update("DROP TABLE IF EXISTS foo");
    conn.update("CREATE TABLE foo (
                    id  BIGINT PRIMARY KEY
                 )");
    conn.update("INSERT INTO foo (id) VALUES (101), (102)");

    do conn.query("SELECT id FROM foo") |it| {
        for it.advance |row| {
            printfln!("%u %d", row.len(), row.get(0).get());
        }
    };
}
