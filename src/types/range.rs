//! Types dealing with ranges of values
#[macro_escape];

extern crate extra;

use std::cmp;
use std::i32;
use std::i64;
use time::Timespec;

/// The `quote!` macro can make it easier to create ranges. It roughly mirrors
/// traditional mathematic range syntax.
///
/// # Note
///
/// The `Range`, `RangeBound`, `Inclusive`, and `Exclusive` types must be
/// directly usable at the location the macro is used.
///
/// # Example
///
/// ```rust
/// #[feature(phase)];
///
/// #[phase(syntax, link)]
/// extern crate postgres;
///
/// use postgres::types::range::{Range, RangeBound, Inclusive, Exclusive};
///
/// fn main() {
///     # let mut r: Range<i32>;
///     // a closed interval
///     r = range!('[' 5i32, 10i32 ']');
///     // an open interval
///     r = range!('(' 5i32, 10i32 ')');
///     // half-open intervals
///     r = range!('(' 5i32, 10i32 ']');
///     r = range!('[' 5i32, 10i32 ')');
///     // a closed lower-bounded interval
///     r = range!('[' 5i32, ')');
///     // an open lower-bounded interval
///     r = range!('(' 5i32, ')');
///     // a closed upper-bounded interval
///     r = range!('(', 10i32 ']');
///     // an open upper-bounded interval
///     r = range!('(', 10i32 ')');
///     // an unbounded interval
///     r = range!('(', ')');
///     // an empty interval
///     r = range!(empty);
/// }
#[macro_export]
macro_rules! range(
    (empty) => (Range::empty());
    ('(', ')') => (Range::new(None, None));
    ('(', $h:expr ')') => (
        Range::new(None, Some(RangeBound::new($h, Exclusive)))
    );
    ('(', $h:expr ']') => (
        Range::new(None, Some(RangeBound::new($h, Inclusive)))
    );
    ('(' $l:expr, ')') => (
        Range::new(Some(RangeBound::new($l, Exclusive)), None)
    );
    ('[' $l:expr, ')') => (
        Range::new(Some(RangeBound::new($l, Inclusive)), None)
    );
    ('(' $l:expr, $h:expr ')') => (
        Range::new(Some(RangeBound::new($l, Exclusive)),
                   Some(RangeBound::new($h, Exclusive)))
    );
    ('(' $l:expr, $h:expr ']') => (
        Range::new(Some(RangeBound::new($l, Exclusive)),
                   Some(RangeBound::new($h, Inclusive)))
    );
    ('[' $l:expr, $h:expr ')') => (
        Range::new(Some(RangeBound::new($l, Inclusive)),
                   Some(RangeBound::new($h, Exclusive)))
    );
    ('[' $l:expr, $h:expr ']') => (
        Range::new(Some(RangeBound::new($l, Inclusive)),
                   Some(RangeBound::new($h, Inclusive)))
    )
)

/// A trait that normalizes a range bound for a type
pub trait Normalizable {
    /// Given a range bound, returns the normalized version of that bound. For
    /// discrete types such as i32, the normalized lower bound is always
    /// inclusive and the normalized upper bound is always exclusive. Other
    /// types, such as Timespec, have no normalization process so their
    /// implementation is a no-op.
    ///
    /// The logic here should match the logic performed by the equivalent
    /// Postgres type.
    fn normalize<S: BoundSided>(bound: RangeBound<S, Self>)
            -> RangeBound<S, Self>;
}

macro_rules! bounded_normalizable(
    ($t:ident) => (
        impl Normalizable for $t {
            fn normalize<S: BoundSided>(bound: RangeBound<S, $t>)
                    -> RangeBound<S, $t> {
                match (BoundSided::side(None::<S>), bound.type_) {
                    (Upper, Inclusive) => {
                        assert!(bound.value != $t::MAX);
                        RangeBound::new(bound.value + 1, Exclusive)
                    }
                    (Lower, Exclusive) => {
                        assert!(bound.value != $t::MAX);
                        RangeBound::new(bound.value + 1, Inclusive)
                    }
                    _ => bound
                }
            }
        }
    )
)

bounded_normalizable!(i32)
bounded_normalizable!(i64)

impl Normalizable for Timespec {
    fn normalize<S: BoundSided>(bound: RangeBound<S, Timespec>)
            -> RangeBound<S, Timespec> {
        bound
    }
}

#[deriving(Eq)]
enum BoundSide {
    Upper,
    Lower
}

#[doc(hidden)]
trait BoundSided {
    // param is a hack to get around lack of hints for self type
    fn side(_: Option<Self>) -> BoundSide;
}

/// A tag type representing an upper bound
#[deriving(Eq,Clone)]
pub enum UpperBound {}

/// A tag type representing a lower bound
#[deriving(Eq,Clone)]
pub enum LowerBound {}

impl BoundSided for UpperBound {
    fn side(_: Option<UpperBound>) -> BoundSide {
        Upper
    }
}

impl BoundSided for LowerBound {
    fn side(_: Option<LowerBound>) -> BoundSide {
        Lower
    }
}

/// The type of a range bound
#[deriving(Eq,Clone)]
pub enum BoundType {
    /// The bound includes its value
    Inclusive,
    /// The bound excludes its value
    Exclusive
}

/// Represents a one-sided bound.
///
/// The side is determined by the `S` phantom parameter.
#[deriving(Eq,Clone)]
pub struct RangeBound<S, T> {
    /// The value of the bound
    value: T,
    /// The type of the bound
    type_: BoundType
}

impl<S: BoundSided, T: Ord> Ord for RangeBound<S, T> {
    fn lt(&self, other: &RangeBound<S, T>) -> bool {
        match (BoundSided::side(None::<S>), self.type_, other.type_) {
            (Upper, Exclusive, Inclusive)
            | (Lower, Inclusive, Exclusive) => self.value <= other.value,
            _ => self.value < other.value
        }
    }
}

impl<S: BoundSided, T: Ord> RangeBound<S, T> {
    /// Constructs a new range bound
    pub fn new(value: T, type_: BoundType) -> RangeBound<S, T> {
        RangeBound { value: value, type_: type_ }
    }

    /// Determines if a value lies within the range specified by this bound.
    pub fn in_bounds(&self, value: &T) -> bool {
        match (self.type_, BoundSided::side(None::<S>)) {
            (Inclusive, Upper) => value <= &self.value,
            (Exclusive, Upper) => value < &self.value,
            (Inclusive, Lower) => value >= &self.value,
            (Exclusive, Lower) => value > &self.value,
        }
    }
}

struct OptBound<'a, S, T>(Option<&'a RangeBound<S, T>>);

impl<'a, S: BoundSided, T: Ord> Ord for OptBound<'a, S, T> {
    fn lt(&self, other: &OptBound<'a, S, T>) -> bool {
        match (*self, *other) {
            (OptBound(None), OptBound(None)) => false,
            (OptBound(None), _) => BoundSided::side(None::<S>) == Lower,
            (_, OptBound(None)) => BoundSided::side(None::<S>) == Upper,
            (OptBound(Some(a)), OptBound(Some(b))) => a < b
        }
    }
}

/// Represents a range of values.
#[deriving(Eq,Clone)]
pub enum Range<T> {
    priv Empty,
    priv Normal(Option<RangeBound<LowerBound, T>>,
                Option<RangeBound<UpperBound, T>>)
}

impl<T: Ord+Normalizable> Range<T> {
    /// Creates a new range.
    ///
    /// If a bound is `None`, the range is unbounded in that direction.
    pub fn new(lower: Option<RangeBound<LowerBound, T>>,
               upper: Option<RangeBound<UpperBound, T>>) -> Range<T> {
        let lower = lower.map(|bound| Normalizable::normalize(bound));
        let upper = upper.map(|bound| Normalizable::normalize(bound));

        match (&lower, &upper) {
            (&Some(ref lower), &Some(ref upper)) => {
                let empty = match (lower.type_, upper.type_) {
                    (Inclusive, Inclusive) => lower.value > upper.value,
                    _ => lower.value >= upper.value
                };
                if empty {
                    return Empty;
                }
            }
            _ => {}
        }

        Normal(lower, upper)
    }

    /// Creates a new empty range.
    pub fn empty() -> Range<T> {
        Empty
    }

    /// Determines if this range is the empty range.
    pub fn is_empty(&self) -> bool {
        match *self {
            Empty => true,
            Normal(..) => false
        }
    }

    /// Returns the lower bound if it exists.
    pub fn lower<'a>(&'a self) -> Option<&'a RangeBound<LowerBound, T>> {
        match *self {
            Normal(Some(ref lower), _) => Some(lower),
            _ => None
        }
    }

    /// Returns the upper bound if it exists.
    pub fn upper<'a>(&'a self) -> Option<&'a RangeBound<UpperBound, T>> {
        match *self {
            Normal(_, Some(ref upper)) => Some(upper),
            _ => None
        }
    }

    /// Determines if a value lies within this range.
    pub fn contains(&self, value: &T) -> bool {
        match *self {
            Empty => false,
            Normal(ref lower, ref upper) => {
                lower.as_ref().map_or(true, |b| b.in_bounds(value)) &&
                    upper.as_ref().map_or(true, |b| b.in_bounds(value))
            }
        }
    }

    /// Determines if a range lies completely within this range.
    pub fn contains_range(&self, other: &Range<T>) -> bool {
        if other.is_empty() {
            return true;
        }

        if self.is_empty() {
            return false;
        }

        OptBound(self.lower()) <= OptBound(other.lower()) &&
            OptBound(self.upper()) >= OptBound(other.upper())
    }
}

impl<T: Ord+Normalizable+Clone> Range<T> {
    /// Returns the intersection of this range with another
    pub fn intersect(&self, other: &Range<T>) -> Range<T> {
        if self.is_empty() || other.is_empty() {
            return Range::empty();
        }

        let OptBound(lower) = cmp::max(OptBound(self.lower()),
                                       OptBound(other.lower()));
        let OptBound(upper) = cmp::min(OptBound(self.upper()),
                                       OptBound(other.upper()));

        Range::new(lower.map(|v| v.clone()), upper.map(|v| v.clone()))
    }
}

#[cfg(test)]
mod test {
    use std::i32;

    use super::*;

    #[test]
    fn test_range_bound_lower_lt() {
        fn check(val1: int, inc1: BoundType, val2: int, inc2: BoundType, expected: bool) {
            let a: RangeBound<LowerBound, int> = RangeBound::new(val1, inc1);
            let b: RangeBound<LowerBound, int> = RangeBound::new(val2, inc2);
            assert_eq!(expected, a < b);
        }

        check(1, Inclusive, 2, Exclusive, true);
        check(1, Exclusive, 2, Inclusive, true);
        check(1, Inclusive, 1, Exclusive, true);
        check(2, Inclusive, 1, Inclusive, false);
        check(2, Exclusive, 1, Exclusive, false);
        check(1, Exclusive, 1, Inclusive, false);
        check(1, Exclusive, 1, Exclusive, false);
        check(1, Inclusive, 1, Inclusive, false);
    }

    #[test]
    fn test_range_bound_upper_lt() {
        fn check(val1: int, inc1: BoundType, val2: int, inc2: BoundType, expected: bool) {
            let a: RangeBound<UpperBound, int> = RangeBound::new(val1, inc1);
            let b: RangeBound<UpperBound, int> = RangeBound::new(val2, inc2);
            assert_eq!(expected, a < b);
        }

        check(1, Inclusive, 2, Exclusive, true);
        check(1, Exclusive, 2, Exclusive, true);
        check(1, Exclusive, 1, Inclusive, true);
        check(2, Inclusive, 1, Inclusive, false);
        check(2, Exclusive, 1, Exclusive, false);
        check(1, Inclusive, 1, Exclusive, false);
        check(1, Inclusive, 1, Inclusive, false);
        check(1, Exclusive, 1, Exclusive, false);
    }

    #[test]
    fn test_range_bound_lower_in_bounds() {
        fn check(bound: int, inc: BoundType, val: int, expected: bool) {
            let b: RangeBound<LowerBound, int> = RangeBound::new(bound, inc);
            assert_eq!(expected, b.in_bounds(&val));
        }

        check(1, Inclusive, 1, true);
        check(1, Exclusive, 1, false);
        check(1, Inclusive, 2, true);
        check(1, Inclusive, 0, false);
    }

    #[test]
    fn test_range_bound_upper_in_bounds() {
        fn check(bound: int, inc: BoundType, val: int, expected: bool) {
            let b: RangeBound<UpperBound, int> = RangeBound::new(bound, inc);
            assert_eq!(expected, b.in_bounds(&val));
        }

        check(1, Inclusive, 1, true);
        check(1, Exclusive, 1, false);
        check(1, Inclusive, 2, false);
        check(1, Inclusive, 0, true);
    }

    #[test]
    fn test_range_contains() {
        let r = range!('[' 1i32, 3i32 ']');
        assert!(!r.contains(&4));
        assert!(r.contains(&3));
        assert!(r.contains(&2));
        assert!(r.contains(&1));
        assert!(!r.contains(&0));

        let r = range!('(' 1i32, 3i32 ')');
        assert!(!r.contains(&4));
        assert!(!r.contains(&3));
        assert!(r.contains(&2));
        assert!(!r.contains(&1));
        assert!(!r.contains(&0));

        let r = range!('(', 3i32 ']');
        assert!(!r.contains(&4));
        assert!(r.contains(&2));
        assert!(r.contains(&i32::MIN));

        let r = range!('[' 1i32, ')');
        assert!(r.contains(&i32::MAX));
        assert!(r.contains(&4));
        assert!(!r.contains(&0));

        let r = range!('(', ')');
        assert!(r.contains(&i32::MAX));
        assert!(r.contains(&0i32));
        assert!(r.contains(&i32::MIN));
    }

    #[test]
    fn test_normalize_lower() {
        let r: RangeBound<LowerBound, i32> = RangeBound::new(10i32, Inclusive);
        assert_eq!(RangeBound::new(10i32, Inclusive), Normalizable::normalize(r));

        let r: RangeBound<LowerBound, i32> = RangeBound::new(10i32, Exclusive);
        assert_eq!(RangeBound::new(11i32, Inclusive), Normalizable::normalize(r));
    }

    #[test]
    fn test_normalize_upper() {
        let r: RangeBound<UpperBound, i32> = RangeBound::new(10i32, Inclusive);
        assert_eq!(RangeBound::new(11i32, Exclusive), Normalizable::normalize(r));

        let r: RangeBound<UpperBound, i32> = RangeBound::new(10i32, Exclusive);
        assert_eq!(RangeBound::new(10i32, Exclusive), Normalizable::normalize(r));
    }

    #[test]
    fn test_range_normalizes() {
        let r1 = range!('(' 10i32, 15i32 ']');
        let r2 = range!('[' 11i32, 16i32 ')');
        assert_eq!(r1, r2);
    }

    #[test]
    fn test_range_empty() {
        assert!((range!('(' 9i32, 10i32 ')')).is_empty());
        assert!((range!('[' 10i32, 10i32 ')')).is_empty());
        assert!((range!('(' 10i32, 10i32 ']')).is_empty());
        assert!((range!('[' 10i32, 9i32 ']')).is_empty());
    }

    #[test]
    fn test_intersection() {
        let r1 = range!('[' 10i32, 15i32 ')');
        let r2 = range!('(' 20i32, 25i32 ']');
        assert!(r1.intersect(&r2).is_empty());
        assert!(r2.intersect(&r1).is_empty());
        assert_eq!(r1, r1.intersect(&range!('(', ')')));
        assert_eq!(r1, (range!('(', ')')).intersect(&r1));

        let r2 = range!('(' 10i32, ')');
        let exp = Range::new(r2.lower().map(|v| v.clone()),
                             r1.upper().map(|v| v.clone()));
        assert_eq!(exp, r1.intersect(&r2));
        assert_eq!(exp, r2.intersect(&r1));

        let r2 = range!('(', 15i32 ']');
        assert_eq!(r1, r1.intersect(&r2));
        assert_eq!(r1, r2.intersect(&r1));

        let r2 = range!('[' 11i32, 14i32 ')');
        assert_eq!(r2, r1.intersect(&r2));
        assert_eq!(r2, r2.intersect(&r1));
    }

    #[test]
    fn test_contains_range() {
        assert!(Range::<i32>::empty().contains_range(&Range::empty()));

        let r1 = range!('[' 10i32, 15i32 ')');
        assert!(r1.contains_range(&r1));

        let r2 = range!('(' 10i32, ')');
        assert!(!r1.contains_range(&r2));
        assert!(!r2.contains_range(&r1));

        let r2 = range!('(', 15i32 ']');
        assert!(!r1.contains_range(&r2));
        assert!(r2.contains_range(&r1));
    }
}
