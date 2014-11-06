use postgres::types::array::{DimensionInfo, ArrayBase, Array, MutableArray};

#[test]
fn test_from_vec() {
    let a = ArrayBase::from_vec(vec!(0i, 1, 2), -1);
    assert!([DimensionInfo { len: 3, lower_bound: -1 }][] ==
            a.dimension_info());
    assert_eq!(&0, a.get(-1));
    assert_eq!(&1, a.get(0));
    assert_eq!(&2, a.get(1));
}

#[test]
#[should_fail]
fn test_get_2d_fail() {
    let mut a = ArrayBase::from_vec(vec!(0i, 1, 2), -1);
    a.wrap(1);
    a.get(1);
}

#[test]
#[should_fail]
fn test_2d_slice_range_fail_low() {
    let mut a = ArrayBase::from_vec(vec!(0i, 1, 2), -1);
    a.wrap(1);
    a.slice(0);
}

#[test]
#[should_fail]
fn test_2d_slice_range_fail_high() {
    let mut a = ArrayBase::from_vec(vec!(0i, 1, 2), -1);
    a.wrap(1);
    a.slice(2);
}

#[test]
fn test_2d_slice_get() {
    let mut a = ArrayBase::from_vec(vec!(0i, 1, 2), -1);
    a.wrap(1);
    let s = a.slice(1);
    assert_eq!(&0, s.get(-1));
    assert_eq!(&1, s.get(0));
    assert_eq!(&2, s.get(1));
}

#[test]
#[should_fail]
fn test_push_move_wrong_lower_bound() {
    let mut a = ArrayBase::from_vec(vec!(1i), -1);
    a.push_move(ArrayBase::from_vec(vec!(2), 0));
}

#[test]
#[should_fail]
fn test_push_move_wrong_dims() {
    let mut a = ArrayBase::from_vec(vec!(1i), -1);
    a.wrap(1);
    a.push_move(ArrayBase::from_vec(vec!(1, 2), -1));
}

#[test]
#[should_fail]
fn test_push_move_wrong_dim_count() {
    let mut a = ArrayBase::from_vec(vec!(1i), -1);
    a.wrap(1);
    let mut b = ArrayBase::from_vec(vec!(2), -1);
    b.wrap(1);
    a.push_move(b);
}

#[test]
fn test_push_move_ok() {
    let mut a = ArrayBase::from_vec(vec!(1i, 2), 0);
    a.wrap(0);
    a.push_move(ArrayBase::from_vec(vec!(3, 4), 0));
    let s = a.slice(0);
    assert_eq!(&1, s.get(0));
    assert_eq!(&2, s.get(1));
    let s = a.slice(1);
    assert_eq!(&3, s.get(0));
    assert_eq!(&4, s.get(1));
}

#[test]
fn test_3d() {
    let mut a = ArrayBase::from_vec(vec!(0i, 1), 0);
    a.wrap(0);
    a.push_move(ArrayBase::from_vec(vec!(2, 3), 0));
    a.wrap(0);
    let mut b = ArrayBase::from_vec(vec!(4, 5), 0);
    b.wrap(0);
    b.push_move(ArrayBase::from_vec(vec!(6, 7), 0));
    a.push_move(b);
    let s1 = a.slice(0);
    let s2 = s1.slice(0);
    assert_eq!(&0, s2.get(0));
    assert_eq!(&1, s2.get(1));
    let s2 = s1.slice(1);
    assert_eq!(&2, s2.get(0));
    assert_eq!(&3, s2.get(1));
    let s1 = a.slice(1);
    let s2 = s1.slice(0);
    assert_eq!(&4, s2.get(0));
    assert_eq!(&5, s2.get(1));
    let s2 = s1.slice(1);
    assert_eq!(&6, s2.get(0));
    assert_eq!(&7, s2.get(1));
}

#[test]
fn test_mut() {
    let mut a = ArrayBase::from_vec(vec!(1i, 2), 0);
    a.wrap(0);
    {
        let mut s = a.slice_mut(0);
        *s.get_mut(0) = 3;
    }
    let s = a.slice(0);
    assert_eq!(&3, s.get(0));
}

#[test]
#[should_fail]
fn test_base_overslice() {
    let a = ArrayBase::from_vec(vec!(1i), 0);
    a.slice(0);
}

#[test]
#[should_fail]
fn test_slice_overslice() {
    let mut a = ArrayBase::from_vec(vec!(1i), 0);
    a.wrap(0);
    let s = a.slice(0);
    s.slice(0);
}
