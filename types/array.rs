//! Multi-dimensional arrays with per-dimension specifiable lower bounds

use std::cast;
use std::vec::VecIterator;

#[deriving(Eq, Clone)]
pub struct DimensionInfo {
    len: uint,
    lower_bound: int,
}

pub trait Array<T> {
    fn get_dimension_info<'a>(&'a self) -> &'a [DimensionInfo];
    fn slice<'a>(&'a self, idx: int) -> ArraySlice<'a, T>;
    fn get<'a>(&'a self, idx: int) -> &'a T;
}

pub trait MutableArray<T> : Array<T> {
    fn slice_mut<'a>(&'a mut self, idx: int) -> MutArraySlice<'a, T> {
        MutArraySlice { slice: self.slice(idx) }
    }

    fn get_mut<'a>(&'a mut self, idx: int) -> &'a mut T {
        unsafe { cast::transmute_mut(self.get(idx)) }
    }
}

trait InternalArray<T> : Array<T> {
    fn shift_idx(&self, idx: int) -> uint {
        let shifted_idx = idx - self.get_dimension_info()[0].lower_bound;
        assert!(shifted_idx >= 0, "Out of bounds array access");
        shifted_idx as uint
    }

    fn raw_get<'a>(&'a self, idx: uint, size: uint) -> &'a T;
}

#[deriving(Eq, Clone)]
pub struct ArrayBase<T> {
    priv info: ~[DimensionInfo],
    priv data: ~[T],
}

impl<T> ArrayBase<T> {
    pub fn from_raw(data: ~[T], info: ~[DimensionInfo])
            -> ArrayBase<T> {
        assert!(!info.is_empty(), "Cannot create a 0x0 array");
        assert!(data.len() == info.iter().fold(1, |acc, i| acc * i.len),
                "Size mismatch");
        ArrayBase {
            info: info,
            data: data,
        }
    }

    pub fn from_vec(data: ~[T], lower_bound: int) -> ArrayBase<T> {
        ArrayBase {
            info: ~[DimensionInfo {
                len: data.len(),
                lower_bound: lower_bound
            }],
            data: data
        }
    }

    pub fn wrap(&mut self, lower_bound: int) {
        self.info.unshift(DimensionInfo {
            len: 1,
            lower_bound: lower_bound
        })
    }

    pub fn push_move(&mut self, other: ArrayBase<T>) {
        assert!(self.info.len() - 1 == other.info.len(),
                "Cannot append differently shaped arrays");
        for (info1, info2) in self.info.iter().skip(1).zip(other.info.iter()) {
            assert!(info1 == info2, "Cannot join differently shaped arrays");
        }
        self.info[0].len += 1;
        self.data.push_all_move(other.data);
    }

    pub fn values<'a>(&'a self) -> VecIterator<'a, T> {
        self.data.iter()
    }
}

impl<T> Array<T> for ArrayBase<T> {
    fn get_dimension_info<'a>(&'a self) -> &'a [DimensionInfo] {
        self.info.as_slice()
    }

    fn slice<'a>(&'a self, idx: int) -> ArraySlice<'a, T> {
        assert!(self.info.len() != 1,
                "Attempted to slice a one-dimensional array");
        ArraySlice {
            parent: BaseParent(self),
            idx: self.shift_idx(idx)
        }
    }

    fn get<'a>(&'a self, idx: int) -> &'a T {
        assert!(self.info.len() == 1,
                "Attempted to get from a multi-dimensional array");
        self.raw_get(self.shift_idx(idx), 1)
    }
}

impl<T> MutableArray<T> for ArrayBase<T> {}

impl<T> InternalArray<T> for ArrayBase<T> {
    fn raw_get<'a>(&'a self, idx: uint, _size: uint) -> &'a T {
        &self.data[idx]
    }
}

enum ArrayParent<'parent, T> {
    SliceParent(&'parent ArraySlice<'parent, T>),
    BaseParent(&'parent ArrayBase<T>),
}

pub struct ArraySlice<'parent, T> {
    priv parent: ArrayParent<'parent, T>,
    priv idx: uint,
}

impl<'parent, T> Array<T> for ArraySlice<'parent, T> {
    fn get_dimension_info<'a>(&'a self) -> &'a [DimensionInfo] {
        let info = match self.parent {
            SliceParent(p) => p.get_dimension_info(),
            BaseParent(p) => p.get_dimension_info()
        };
        info.slice_from(1)
    }

    fn slice<'a>(&'a self, idx: int) -> ArraySlice<'a, T> {
        assert!(self.get_dimension_info().len() != 1,
                "Attempted to slice a one-dimensional array");
        ArraySlice {
            parent: SliceParent(self),
            idx: self.shift_idx(idx)
        }
    }

    fn get<'a>(&'a self, idx: int) -> &'a T {
        assert!(self.get_dimension_info().len() == 1,
                "Attempted to get from a multi-dimensional array");
        self.raw_get(self.shift_idx(idx), 1)
    }
}

impl<'parent, T> InternalArray<T> for ArraySlice<'parent, T> {
    fn raw_get<'a>(&'a self, idx: uint, size: uint) -> &'a T {
        let size = size * self.get_dimension_info()[0].len;
        let idx = size * self.idx + idx;
        match self.parent {
            SliceParent(p) => p.raw_get(idx, size),
            BaseParent(p) => p.raw_get(idx, size)
        }
    }
}

pub struct MutArraySlice<'parent, T> {
    priv slice: ArraySlice<'parent, T>
}

impl<'parent, T> Array<T> for MutArraySlice<'parent, T> {
    fn get_dimension_info<'a>(&'a self) -> &'a [DimensionInfo] {
        self.slice.get_dimension_info()
    }

    fn slice<'a>(&'a self, idx: int) -> ArraySlice<'a, T> {
        self.slice.slice(idx)
    }

    fn get<'a>(&'a self, idx: int) -> &'a T {
        self.slice.get(idx)
    }
}

impl<'parent, T> MutableArray<T> for MutArraySlice<'parent, T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_vec() {
        let a = ArrayBase::from_vec(~[0, 1, 2], -1);
        assert_eq!([DimensionInfo { len: 3, lower_bound: -1 }],
                   a.get_dimension_info());
        assert_eq!(&0, a.get(-1));
        assert_eq!(&1, a.get(0));
        assert_eq!(&2, a.get(1));
    }

    #[test]
    #[should_fail]
    fn test_get_2d_fail() {
        let mut a = ArrayBase::from_vec(~[0, 1, 2], -1);
        a.wrap(1);
        a.get(1);
    }

    #[test]
    #[should_fail]
    fn test_2d_slice_range_fail() {
        let mut a = ArrayBase::from_vec(~[0, 1, 2], -1);
        a.wrap(1);
        a.slice(0);
    }

    #[test]
    fn test_2d_slice_get() {
        let mut a = ArrayBase::from_vec(~[0, 1, 2], -1);
        a.wrap(1);
        let s = a.slice(1);
        assert_eq!(&0, s.get(-1));
        assert_eq!(&1, s.get(0));
        assert_eq!(&2, s.get(1));
    }

    #[test]
    #[should_fail]
    fn test_push_move_wrong_lower_bound() {
        let mut a = ArrayBase::from_vec(~[1], -1);
        a.push_move(ArrayBase::from_vec(~[2], 0));
    }

    #[test]
    #[should_fail]
    fn test_push_move_wrong_dims() {
        let mut a = ArrayBase::from_vec(~[1], -1);
        a.wrap(1);
        a.push_move(ArrayBase::from_vec(~[1, 2], -1));
    }

    #[test]
    #[should_fail]
    fn test_push_move_wrong_dim_count() {
        let mut a = ArrayBase::from_vec(~[1], -1);
        a.wrap(1);
        let mut b = ArrayBase::from_vec(~[2], -1);
        b.wrap(1);
        a.push_move(b);
    }

    #[test]
    fn test_push_move_ok() {
        let mut a = ArrayBase::from_vec(~[1, 2], 0);
        a.wrap(0);
        a.push_move(ArrayBase::from_vec(~[3, 4], 0));
        let s = a.slice(0);
        assert_eq!(&1, s.get(0));
        assert_eq!(&2, s.get(1));
        let s = a.slice(1);
        assert_eq!(&3, s.get(0));
        assert_eq!(&4, s.get(1));
    }

    #[test]
    fn test_3d() {
        let mut a = ArrayBase::from_vec(~[0, 1], 0);
        a.wrap(0);
        a.push_move(ArrayBase::from_vec(~[2, 3], 0));
        a.wrap(0);
        let mut b = ArrayBase::from_vec(~[4, 5], 0);
        b.wrap(0);
        b.push_move(ArrayBase::from_vec(~[6, 7], 0));
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
        let mut a = ArrayBase::from_vec(~[1, 2], 0);
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
        let a = ArrayBase::from_vec(~[1], 0);
        a.slice(0);
    }

    #[test]
    #[should_fail]
    fn test_slice_overslice() {
        let mut a = ArrayBase::from_vec(~[1], 0);
        a.wrap(0);
        let s = a.slice(0);
        s.slice(0);
    }
}
