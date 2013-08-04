
pub trait SqlConnection<'self, S: SqlStatement> {
    fn new(uri: &str) -> ~Self;

    fn prepare(&self, query: &str) -> ~S<'self>;
}

pub trait SqlStatement {
    fn query<T>(&self, params: &[@SqlType],
                blk: &fn(&SqlResult) -> Result<T, ~str>)
                -> Result<T, ~str>;

    fn update(&self, params: &[@SqlType]) -> Result<uint, ~str>;
}

pub trait SqlResult<R: SqlRow, I: Iterator<R>>: Container {
    fn iter(&self) -> I;
}

pub trait SqlRow {
    fn get<T: SqlType>(&self) -> Option<T>;
}

pub enum SqlType {
    Int(int),
    Uint(uint),
}
