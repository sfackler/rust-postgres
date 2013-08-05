
pub trait SqlConnection<Row, It, Res, S: SqlStatement<Row, It, Res>> {
    fn new(uri: &str) -> Result<~Self, ~str>;
    fn prepare<'a>(&'a self, query: &str) -> Result<~S, ~str>;
}

pub trait SqlStatement<Row, It, R: SqlResult<Row, It>> {
    fn query<T>(&self, params: &[~str], blk: &fn(&R) -> Result<T, ~str>)
                -> Result<T, ~str>;
    fn update(&self, params: &[~str]) -> Result<uint, ~str>;
}

pub trait SqlResult<R: SqlRow, I: Iterator<R>>: Container {
    fn iter(&self) -> I;
}

pub trait SqlRow: Container {
    fn get<T: FromSql<Self>>(&self, index: uint) -> Option<T>;
}

pub trait FromSql<R/*: SqlRow*/> {
    fn from_sql(row: &R, column: uint) -> Option<Self>;
}
