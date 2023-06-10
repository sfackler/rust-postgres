use crate::test_type;
use postgres::{Client, NoTls};
use postgres_types::{FromSql, ToSql, WrongType};
use std::error::Error;

#[test]
fn defaults() {
    #[derive(Debug, ToSql, FromSql, PartialEq)]
    enum Foo {
        Bar,
        Baz,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.execute("CREATE TYPE pg_temp.\"Foo\" AS ENUM ('Bar', 'Baz')", &[])
        .unwrap();

    test_type(
        &mut conn,
        "\"Foo\"",
        &[(Foo::Bar, "'Bar'"), (Foo::Baz, "'Baz'")],
    );
}

#[test]
fn name_overrides() {
    #[derive(Debug, ToSql, FromSql, PartialEq)]
    #[postgres(name = "mood")]
    enum Mood {
        #[postgres(name = "sad")]
        Sad,
        #[postgres(name = "ok")]
        Ok,
        #[postgres(name = "happy")]
        Happy,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.execute(
        "CREATE TYPE pg_temp.mood AS ENUM ('sad', 'ok', 'happy')",
        &[],
    )
    .unwrap();

    test_type(
        &mut conn,
        "mood",
        &[
            (Mood::Sad, "'sad'"),
            (Mood::Ok, "'ok'"),
            (Mood::Happy, "'happy'"),
        ],
    );
}

#[test]
fn rename_all_overrides() {
    #[derive(Debug, ToSql, FromSql, PartialEq)]
    #[postgres(name = "mood", rename_all = "snake_case")]
    enum Mood {
        VerySad,
        #[postgres(name = "okay")]
        Ok,
        VeryHappy,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.execute(
        "CREATE TYPE pg_temp.mood AS ENUM ('very_sad', 'okay', 'very_happy')",
        &[],
    )
    .unwrap();

    test_type(
        &mut conn,
        "mood",
        &[
            (Mood::VerySad, "'very_sad'"),
            (Mood::Ok, "'okay'"),
            (Mood::VeryHappy, "'very_happy'"),
        ],
    );
}

#[test]
fn wrong_name() {
    #[derive(Debug, ToSql, FromSql, PartialEq)]
    enum Foo {
        Bar,
        Baz,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.execute("CREATE TYPE pg_temp.foo AS ENUM ('Bar', 'Baz')", &[])
        .unwrap();

    let err = conn.execute("SELECT $1::foo", &[&Foo::Bar]).unwrap_err();
    assert!(err.source().unwrap().is::<WrongType>());
}

#[test]
fn extra_variant() {
    #[derive(Debug, ToSql, FromSql, PartialEq)]
    #[postgres(name = "foo")]
    enum Foo {
        Bar,
        Baz,
        Buz,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.execute("CREATE TYPE pg_temp.foo AS ENUM ('Bar', 'Baz')", &[])
        .unwrap();

    let err = conn.execute("SELECT $1::foo", &[&Foo::Bar]).unwrap_err();
    assert!(err.source().unwrap().is::<WrongType>());
}

#[test]
fn missing_variant() {
    #[derive(Debug, ToSql, FromSql, PartialEq)]
    #[postgres(name = "foo")]
    enum Foo {
        Bar,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.execute("CREATE TYPE pg_temp.foo AS ENUM ('Bar', 'Baz')", &[])
        .unwrap();

    let err = conn.execute("SELECT $1::foo", &[&Foo::Bar]).unwrap_err();
    assert!(err.source().unwrap().is::<WrongType>());
}
