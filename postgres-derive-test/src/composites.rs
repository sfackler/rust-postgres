use crate::test_type;
use postgres::{Client, NoTls};
use postgres_types::{FromSql, ToSql, WrongType};
use std::error::Error;

#[test]
fn defaults() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    struct InventoryItem {
        name: String,
        supplier_id: i32,
        price: Option<f64>,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.batch_execute(
        "CREATE TYPE pg_temp.\"InventoryItem\" AS (
            name TEXT,
            supplier_id INT,
            price DOUBLE PRECISION
        );",
    )
    .unwrap();

    let item = InventoryItem {
        name: "foobar".to_owned(),
        supplier_id: 100,
        price: Some(15.50),
    };

    let item_null = InventoryItem {
        name: "foobar".to_owned(),
        supplier_id: 100,
        price: None,
    };

    test_type(
        &mut conn,
        "\"InventoryItem\"",
        &[
            (item, "ROW('foobar', 100, 15.50)"),
            (item_null, "ROW('foobar', 100, NULL)"),
        ],
    );
}

#[test]
fn name_overrides() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    #[postgres(name = "inventory_item")]
    struct InventoryItem {
        #[postgres(name = "name")]
        _name: String,
        #[postgres(name = "supplier_id")]
        _supplier_id: i32,
        #[postgres(name = "price")]
        _price: Option<f64>,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.batch_execute(
        "CREATE TYPE pg_temp.inventory_item AS (
            name TEXT,
            supplier_id INT,
            price DOUBLE PRECISION
        );",
    )
    .unwrap();

    let item = InventoryItem {
        _name: "foobar".to_owned(),
        _supplier_id: 100,
        _price: Some(15.50),
    };

    let item_null = InventoryItem {
        _name: "foobar".to_owned(),
        _supplier_id: 100,
        _price: None,
    };

    test_type(
        &mut conn,
        "inventory_item",
        &[
            (item, "ROW('foobar', 100, 15.50)"),
            (item_null, "ROW('foobar', 100, NULL)"),
        ],
    );
}

#[test]
fn wrong_name() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    struct InventoryItem {
        name: String,
        supplier_id: i32,
        price: Option<f64>,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.batch_execute(
        "CREATE TYPE pg_temp.inventory_item AS (
            name TEXT,
            supplier_id INT,
            price DOUBLE PRECISION
        );",
    )
    .unwrap();

    let item = InventoryItem {
        name: "foobar".to_owned(),
        supplier_id: 100,
        price: Some(15.50),
    };

    let err = conn
        .execute("SELECT $1::inventory_item", &[&item])
        .unwrap_err();
    assert!(err.source().unwrap().is::<WrongType>());
}

#[test]
fn extra_field() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    #[postgres(name = "inventory_item")]
    struct InventoryItem {
        name: String,
        supplier_id: i32,
        price: Option<f64>,
        foo: i32,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.batch_execute(
        "CREATE TYPE pg_temp.inventory_item AS (
            name TEXT,
            supplier_id INT,
            price DOUBLE PRECISION
        );",
    )
    .unwrap();

    let item = InventoryItem {
        name: "foobar".to_owned(),
        supplier_id: 100,
        price: Some(15.50),
        foo: 0,
    };

    let err = conn
        .execute("SELECT $1::inventory_item", &[&item])
        .unwrap_err();
    assert!(err.source().unwrap().is::<WrongType>());
}

#[test]
fn missing_field() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    #[postgres(name = "inventory_item")]
    struct InventoryItem {
        name: String,
        supplier_id: i32,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.batch_execute(
        "CREATE TYPE pg_temp.inventory_item AS (
            name TEXT,
            supplier_id INT,
            price DOUBLE PRECISION
        );",
    )
    .unwrap();

    let item = InventoryItem {
        name: "foobar".to_owned(),
        supplier_id: 100,
    };

    let err = conn
        .execute("SELECT $1::inventory_item", &[&item])
        .unwrap_err();
    assert!(err.source().unwrap().is::<WrongType>());
}

#[test]
fn wrong_type() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    #[postgres(name = "inventory_item")]
    struct InventoryItem {
        name: String,
        supplier_id: i32,
        price: i32,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.batch_execute(
        "CREATE TYPE pg_temp.inventory_item AS (
            name TEXT,
            supplier_id INT,
            price DOUBLE PRECISION
        );",
    )
    .unwrap();

    let item = InventoryItem {
        name: "foobar".to_owned(),
        supplier_id: 100,
        price: 0,
    };

    let err = conn
        .execute("SELECT $1::inventory_item", &[&item])
        .unwrap_err();
    assert!(err.source().unwrap().is::<WrongType>());
}

#[test]
fn struct_with_references() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    #[postgres(name = "item")]
    struct Item<'a, 'b: 'a> {
        name: &'a str,
        data: &'b [u8],
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.batch_execute(
        "CREATE TYPE pg_temp.item AS (
            name TEXT,
            data BYTEA
        );",
    )
    .unwrap();

    let item = Item {
        name: "foobar",
        data: b"12345",
    };

    let row = conn.query_one("SELECT $1::item", &[&item]).unwrap();
    let result: Item<'_, '_> = row.get(0);
    assert_eq!(item.name, result.name);
    assert_eq!(item.data, result.data);
}
