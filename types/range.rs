#[allow(missing_doc)];

enum BoundSide {
    Upper,
    Lower
}

trait BoundSided {
    // param is a hack to get around lack of hints for self type
    fn side(_: Option<Self>) -> BoundSide;
}

pub struct UpperBound;
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

pub enum BoundType {
    Inclusive,
    Exclusive
}

pub struct RangeBound<S, T> {
    value: T,
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
    pub fn new(value: T, type_: BoundType) -> RangeBound<S, T> {
        RangeBound { value: value, type_: type_ }
    }

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

#[deriving(Eq)]
pub enum RangeComparison {
    Above,
    Within,
    Below
}

pub struct Range<T> {
    lower: Option<RangeBound<LowerBound, T>>,
    upper: Option<RangeBound<UpperBound, T>>,
}

impl<T: Ord> Range<T> {
    pub fn new(lower: Option<RangeBound<LowerBound, T>>,
               upper: Option<RangeBound<UpperBound, T>>) -> Range<T> {
        match (&lower, &upper) {
            (&Some(ref lower), &Some(ref upper)) =>
                assert!(lower.value <= upper.value),
            _ => {}
        }

        Range { lower: lower, upper: upper }
    }

    pub fn cmp(&self, value: &T) -> RangeComparison {
        let lower = do self.lower.as_ref().map_default(true) |b| {
            b.in_bounds(value)
        };
        let upper = do self.upper.as_ref().map_default(true) |b| {
            b.in_bounds(value)
        };

        match (lower, upper) {
            (true, false) => Above,
            (true, true) => Within,
            (false, true) => Below,
            _ => unreachable!()
        }
    }

    pub fn contains(&self, value: &T) -> bool {
        self.cmp(value) == Within
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::int;

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
    fn test_range_cmp() {
        let r =  Range::new(Some(RangeBound::new(1, Inclusive)),
                            Some(RangeBound::new(3, Inclusive)));
        assert_eq!(Above, r.cmp(&4));
        assert_eq!(Within, r.cmp(&3));
        assert_eq!(Within, r.cmp(&2));
        assert_eq!(Within, r.cmp(&1));
        assert_eq!(Below, r.cmp(&0));

        let r =  Range::new(Some(RangeBound::new(1, Exclusive)),
                            Some(RangeBound::new(3, Exclusive)));
        assert_eq!(Above, r.cmp(&4));
        assert_eq!(Above, r.cmp(&3));
        assert_eq!(Within, r.cmp(&2));
        assert_eq!(Below, r.cmp(&1));
        assert_eq!(Below, r.cmp(&0));

        let r = Range::new(None, Some(RangeBound::new(3, Inclusive)));
        assert_eq!(Above, r.cmp(&4));
        assert_eq!(Within, r.cmp(&2));
        assert_eq!(Within, r.cmp(&int::min_value));

        let r = Range::new(Some(RangeBound::new(1, Inclusive)), None);
        assert_eq!(Within, r.cmp(&int::max_value));
        assert_eq!(Within, r.cmp(&4));
        assert_eq!(Below, r.cmp(&0));

        let r = Range::new(None, None);
        assert_eq!(Within, r.cmp(&int::max_value));
        assert_eq!(Within, r.cmp(&0));
        assert_eq!(Within, r.cmp(&int::min_value));
    }
}
