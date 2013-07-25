
// pub trait Connection<'self, R: Rows<'self>> {
//     fn query(&self, query: &str, params: &[@ToSqlStr]) -> Result<~R, ~str>;
// }

// pub trait Rows<'self, R: Row, I: Iterator<&'self R>>: Container {
//     fn iter(&self) -> I;
// }

// pub trait Row: Container {
//     fn get<T: FromSqlStr>(&self) -> Option<T>;
// }

pub trait ToSqlStr {
    fn to_sql_str(&self) -> ~str;
}

impl ToSqlStr for int {
    fn to_sql_str(&self) -> ~str {
        self.to_str()
    }
}

impl ToSqlStr for uint {
    fn to_sql_str(&self) -> ~str {
        self.to_str()
    }
}

pub trait FromSqlStr {
    fn from_sql_str(&str) -> Option<Self>;
}

impl FromSqlStr for int {
    fn from_sql_str(s: &str) -> Option<int> {
        FromStr::from_str(s)
    }
}

impl FromSqlStr for uint {
    fn from_sql_str(s: &str) -> Option<uint> {
        FromStr::from_str(s)
    }
}
