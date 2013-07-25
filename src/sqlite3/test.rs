extern mod sqlite3;

#[test]
fn test() {
    let conn = sqlite3::open("db").unwrap();
    conn.prepare("DROP TABLE IF EXISTS foo").unwrap().update();
    conn.prepare("CREATE TABLE foo (
                    id  BIGINT PRIMARY KEY
                  )").unwrap().update();
    conn.prepare("INSERT INTO foo (id) VALUES (101), (102)").unwrap().update();
    let stmt = conn.prepare("SELECT id FROM foo").unwrap();
    for stmt.query().unwrap().advance |row| {
        printfln!("%u %d", row.len(), row.get(0).get());
    }
}
