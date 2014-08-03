use std::i32;

use postgres::types::range::{RangeBound,
                             Range,
                             Inclusive,
                             Exclusive,
                             UpperBound,
                             LowerBound,
                             Normalizable,
                             BoundType};

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
fn test_union() {
    let r1 = range!('[' 10i32, 15i32 ')');
    let r2 = range!('(' 20i32, 25i32 ']');
    assert_eq!(None, r1.union(&r2));
    assert_eq!(None, r2.union(&r1));

    let r2 = range!('(', ')');
    assert_eq!(Some(r2), r1.union(&r2));
    assert_eq!(Some(r2), r2.union(&r1));

    let r2 = range!('[' 13i32, 50i32 ')');
    assert_eq!(Some(range!('[' 10i32, 50i32 ')')), r1.union(&r2));
    assert_eq!(Some(range!('[' 10i32, 50i32 ')')), r2.union(&r1));

    let r2 = range!('[' 3i32, 50i32 ')');
    assert_eq!(Some(range!('[' 3i32, 50i32 ')')), r1.union(&r2));
    assert_eq!(Some(range!('[' 3i32, 50i32 ')')), r2.union(&r1));

    let r2 = range!('(', 11i32 ')');
    assert_eq!(Some(range!('(', 15i32 ')')), r1.union(&r2));
    assert_eq!(Some(range!('(', 15i32 ')')), r2.union(&r1));

    let r2 = range!('(' 11i32, ')');
    assert_eq!(Some(range!('[' 10i32, ')')), r1.union(&r2));
    assert_eq!(Some(range!('[' 10i32, ')')), r2.union(&r1));

    let r2 = range!('(' 15i32, 20i32 ')');
    assert_eq!(None, r1.union(&r2));
    assert_eq!(None, r2.union(&r1));

    let r2 = range!('[' 15i32, 20i32 ']');
    assert_eq!(Some(range!('[' 10i32, 20i32 ']')), r1.union(&r2));
    assert_eq!(Some(range!('[' 10i32, 20i32 ']')), r2.union(&r1));
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
