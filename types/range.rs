//! Types dealing with ranges of values

extern mod extra;

use std::cmp;
use extra::time::Timespec;

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
    ($t:ty) => (
        impl Normalizable for $t {
            fn normalize<S: BoundSided>(bound: RangeBound<S, $t>)
                    -> RangeBound<S, $t> {
                match BoundSided::side(None::<S>) {
                    Upper if bound.type_ == Inclusive => {
                        assert!(bound.value != Bounded::max_value());
                        RangeBound::new(bound.value + 1, Exclusive)
                    }
                    Lower if bound.type_ == Exclusive => {
                        assert!(bound.value != Bounded::max_value());
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

trait BoundSided {
    // param is a hack to get around lack of hints for self type
    fn side(_: Option<Self>) -> BoundSide;
}

/// A tag type representing an upper bound
#[deriving(Eq,Clone)]
pub struct UpperBound;

/// A tag type representing a lower bound
#[deriving(Eq,Clone)]
pub struct LowerBound;

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
            (Inclusive, Upper) if value <= &self.value => true,
            (Exclusive, Upper) if value < &self.value => true,
            (Inclusive, Lower) if value >= &self.value => true,
            (Exclusive, Lower) if value > &self.value => true,
            _ => false
        }
    }
}

struct OptBound<'a, S, T>(&'a Option<RangeBound<S, T>>);

impl<'a, S: BoundSided, T: Ord> Ord for OptBound<'a, S, T> {
    fn lt(&self, other: &OptBound<'a, S, T>) -> bool {
        match (**self, **other) {
            (&None, &None) => false,
            (&None, _) => BoundSided::side(None::<S>) == Lower,
            (_, &None) => BoundSided::side(None::<S>) == Upper,
            (&Some(ref a), &Some(ref b)) => a < b
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
    pub fn lower<'a>(&'a self) -> &'a Option<RangeBound<LowerBound, T>> {
        match *self {
            Empty => &None,
            Normal(ref lower, _) => lower
        }
    }

    /// Returns the upper bound if it exists.
    pub fn upper<'a>(&'a self) -> &'a Option<RangeBound<UpperBound, T>> {
        match *self {
            Empty => &None,
            Normal(_, ref upper) => upper
        }
    }

    /// Determines if a value lies within this range.
    pub fn contains(&self, value: &T) -> bool {
        match *self {
            Empty => false,
            Normal(ref lower, ref upper) => {
                lower.as_ref().map_default(true, |b| b.in_bounds(value)) &&
                    upper.as_ref().map_default(true, |b| b.in_bounds(value))
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

        let lower = cmp::max(OptBound(self.lower()), OptBound(other.lower()))
                .clone();
        let upper = cmp::min(OptBound(self.upper()), OptBound(other.upper()))
                .clone();

        Range::new(lower, upper)
    }
}

#[cfg(test)]
mod test {
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
        let r =  Range::new(Some(RangeBound::new(1i32, Inclusive)),
                            Some(RangeBound::new(3i32, Inclusive)));
        assert!(!r.contains(&4));
        assert!(r.contains(&3));
        assert!(r.contains(&2));
        assert!(r.contains(&1));
        assert!(!r.contains(&0));

        let r =  Range::new(Some(RangeBound::new(1i32, Exclusive)),
                            Some(RangeBound::new(3i32, Exclusive)));
        assert!(!r.contains(&4));
        assert!(!r.contains(&3));
        assert!(r.contains(&2));
        assert!(!r.contains(&1));
        assert!(!r.contains(&0));

        let r = Range::new(None, Some(RangeBound::new(3i32, Inclusive)));
        assert!(!r.contains(&4));
        assert!(r.contains(&2));
        assert!(r.contains(&Bounded::min_value()));

        let r = Range::new(Some(RangeBound::new(1i32, Inclusive)), None);
        assert!(r.contains(&Bounded::max_value()));
        assert!(r.contains(&4));
        assert!(!r.contains(&0));

        let r = Range::new(None, None);
        assert!(r.contains(&Bounded::max_value()));
        assert!(r.contains(&0i32));
        assert!(r.contains(&Bounded::min_value()));
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
        let r1 = Range::new(Some(RangeBound::new(10i32, Exclusive)),
                            Some(RangeBound::new(15i32, Inclusive)));
        let r2 = Range::new(Some(RangeBound::new(11i32, Inclusive)),
                            Some(RangeBound::new(16i32, Exclusive)));
        assert_eq!(r1, r2);
    }

    #[test]
    fn test_range_empty() {
        assert!(Range::new(Some(RangeBound::new(9i32, Exclusive)),
                           Some(RangeBound::new(10i32, Exclusive))).is_empty());
        assert!(Range::new(Some(RangeBound::new(10i32, Inclusive)),
                           Some(RangeBound::new(10i32, Exclusive))).is_empty());
        assert!(Range::new(Some(RangeBound::new(10i32, Exclusive)),
                           Some(RangeBound::new(10i32, Inclusive))).is_empty());
        assert!(Range::new(Some(RangeBound::new(10i32, Inclusive)),
                           Some(RangeBound::new(9i32, Inclusive))).is_empty());
    }

    #[test]
    fn test_intersection() {
        let r1 = Range::new(Some(RangeBound::new(10i32, Inclusive)),
                            Some(RangeBound::new(15i32, Exclusive)));
        let r2 = Range::new(Some(RangeBound::new(20i32, Exclusive)),
                            Some(RangeBound::new(25i32, Inclusive)));
        assert!(r1.intersect(&r2).is_empty());
        assert!(r2.intersect(&r1).is_empty());
        assert_eq!(r1, r1.intersect(&Range::new(None, None)));
        assert_eq!(r1, Range::new(None, None).intersect(&r1));

        let r2 = Range::new(Some(RangeBound::new(10i32, Exclusive)), None);
        let exp = Range::new(r2.lower().clone(), r1.upper().clone());
        assert_eq!(exp, r1.intersect(&r2));
        assert_eq!(exp, r2.intersect(&r1));

        let r2 = Range::new(None, Some(RangeBound::new(15i32, Inclusive)));
        assert_eq!(r1, r1.intersect(&r2));
        assert_eq!(r1, r2.intersect(&r1));

        let r2 = Range::new(Some(RangeBound::new(11i32, Inclusive)),
                            Some(RangeBound::new(14i32, Exclusive)));
        assert_eq!(r2, r1.intersect(&r2));
        assert_eq!(r2, r2.intersect(&r1));
    }

    #[test]
    fn test_contains_range() {
        assert!(Range::<i32>::empty().contains_range(&Range::empty()));

        let r1 = Range::new(Some(RangeBound::new(10i32, Inclusive)),
                           Some(RangeBound::new(15i32, Exclusive)));
        assert!(r1.contains_range(&r1));

        let r2 = Range::new(Some(RangeBound::new(10i32, Exclusive)),
                            None);
        assert!(!r1.contains_range(&r2));
        assert!(!r2.contains_range(&r1));

        let r2 = Range::new(None,
                            Some(RangeBound::new(15i32, Inclusive)));
        assert!(!r1.contains_range(&r2));
        assert!(r2.contains_range(&r1));
    }
}
