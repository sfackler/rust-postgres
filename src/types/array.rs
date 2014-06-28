//! Multi-dimensional arrays with per-dimension specifiable lower bounds

use std::mem;
use std::slice;

/// Information about a dimension of an array
#[deriving(PartialEq, Eq, Clone, Show)]
pub struct DimensionInfo {
    /// The size of the dimension
    pub len: uint,
    /// The index of the first element of the dimension
    pub lower_bound: int,
}

/// Specifies methods that can be performed on multi-dimensional arrays
pub trait Array<T> {
    /// Returns information about the dimensions of this array
    fn dimension_info<'a>(&'a self) -> &'a [DimensionInfo];

    /// Slices into this array, returning an immutable view of a subarray.
    ///
    /// # Failure
    ///
    /// Fails if the array is one-dimensional or the index is out of bounds.
    fn slice<'a>(&'a self, idx: int) -> ArraySlice<'a, T>;

    /// Retrieves an immutable reference to a value in this array.
    ///
    ///
    /// # Failure
    ///
    /// Fails if the array is multi-dimensional or the index is out of bounds.
    fn get<'a>(&'a self, idx: int) -> &'a T;
}

/// Specifies methods that can be performed on mutable multi-dimensional arrays
pub trait MutableArray<T> : Array<T> {
    /// Slices into this array, returning a mutable view of a subarray.
    ///
    /// # Failure
    ///
    /// Fails if the array is one-dimensional or the index is out of bounds.
    fn slice_mut<'a>(&'a mut self, idx: int) -> MutArraySlice<'a, T>;

    /// Retrieves a mutable reference to a value in this array.
    ///
    ///
    /// # Failure
    ///
    /// Fails if the array is multi-dimensional or the index is out of bounds.
    fn get_mut<'a>(&'a mut self, idx: int) -> &'a mut T;
}

#[doc(hidden)]
trait InternalArray<T>: Array<T> {
    fn shift_idx(&self, idx: int) -> uint {
        let shifted_idx = idx - self.dimension_info()[0].lower_bound;
        assert!(shifted_idx >= 0 &&
                    shifted_idx < self.dimension_info()[0].len as int,
                "Out of bounds array access");
        shifted_idx as uint
    }

    fn raw_get<'a>(&'a self, idx: uint, size: uint) -> &'a T;
}

#[doc(hidden)]
trait InternalMutableArray<T>: MutableArray<T> {
    fn raw_get_mut<'a>(&'a mut self, idx: uint, size: uint) -> &'a mut T;
}

/// A multi-dimensional array
#[deriving(PartialEq, Eq, Clone)]
pub struct ArrayBase<T> {
    info: Vec<DimensionInfo>,
    data: Vec<T>,
}

impl<T> ArrayBase<T> {
    /// Creates a new multi-dimensional array from its underlying components.
    ///
    /// The data array should be provided in the higher-dimensional equivalent
    /// of row-major order.
    ///
    /// # Failure
    ///
    /// Fails if there are 0 dimensions or the number of elements provided does
    /// not match the number of elements specified.
    pub fn from_raw(data: Vec<T>, info: Vec<DimensionInfo>)
            -> ArrayBase<T> {
        assert!(!info.is_empty(), "Cannot create a 0x0 array");
        assert!(data.len() == info.iter().fold(1, |acc, i| acc * i.len),
                "Size mismatch");
        ArrayBase {
            info: info,
            data: data,
        }
    }

    /// Creates a new one-dimensional array from a vector.
    pub fn from_vec(data: Vec<T>, lower_bound: int) -> ArrayBase<T> {
        ArrayBase {
            info: vec!(DimensionInfo {
                len: data.len(),
                lower_bound: lower_bound
            }),
            data: data
        }
    }

    /// Wraps this array in a new dimension of size 1.
    ///
    /// For example the one-dimensional array `[1,2]` would turn into
    /// the two-dimensional array `[[1,2]]`.
    pub fn wrap(&mut self, lower_bound: int) {
        self.info.unshift(DimensionInfo {
            len: 1,
            lower_bound: lower_bound
        })
    }

    /// Takes ownership of another array, appending it to the top-level
    /// dimension of this array.
    ///
    /// The dimensions of the other array must have an identical shape to the
    /// dimensions of a slice of this array. This includes both the sizes of
    /// the dimensions as well as their lower bounds.
    ///
    /// For example, if `[3,4]` is pushed onto `[[1,2]]`, the result is
    /// `[[1,2],[3,4]]`.
    ///
    /// # Failure
    ///
    /// Fails if the other array does not have dimensions identical to the
    /// dimensions of a slice of this array.
    pub fn push_move(&mut self, other: ArrayBase<T>) {
        assert!(self.info.len() - 1 == other.info.len(),
                "Cannot append differently shaped arrays");
        for (info1, info2) in self.info.iter().skip(1).zip(other.info.iter()) {
            assert!(info1 == info2, "Cannot join differently shaped arrays");
        }
        self.info.get_mut(0).len += 1;
        self.data.push_all_move(other.data);
    }

    /// Returns an iterator over the values in this array, in the
    /// higher-dimensional equivalent of row-major order.
    pub fn values<'a>(&'a self) -> slice::Items<'a, T> {
        self.data.iter()
    }
}

impl<T> Array<T> for ArrayBase<T> {
    fn dimension_info<'a>(&'a self) -> &'a [DimensionInfo] {
        self.info.as_slice()
    }

    fn slice<'a>(&'a self, idx: int) -> ArraySlice<'a, T> {
        assert!(self.info.len() != 1,
                "Attempted to slice a one-dimensional array");
        ArraySlice {
            parent: BaseParent(self),
            idx: self.shift_idx(idx),
        }
    }

    fn get<'a>(&'a self, idx: int) -> &'a T {
        assert!(self.info.len() == 1,
                "Attempted to get from a multi-dimensional array");
        self.raw_get(self.shift_idx(idx), 1)
    }
}

impl<T> MutableArray<T> for ArrayBase<T> {
    fn slice_mut<'a>(&'a mut self, idx: int) -> MutArraySlice<'a, T> {
        assert!(self.info.len() != 1,
                "Attempted to slice_mut into a one-dimensional array");
        MutArraySlice {
            idx: self.shift_idx(idx),
            parent: MutBaseParent(self),
        }
    }

    fn get_mut<'a>(&'a mut self, idx: int) -> &'a mut T {
        assert!(self.info.len() == 1,
                "Attempted to get_mut from a multi-dimensional array");
        let idx = self.shift_idx(idx);
        self.raw_get_mut(idx, 1)
    }
}

impl<T> InternalArray<T> for ArrayBase<T> {
    fn raw_get<'a>(&'a self, idx: uint, _size: uint) -> &'a T {
        self.data.get(idx)
    }
}

impl<T> InternalMutableArray<T> for ArrayBase<T> {
    fn raw_get_mut<'a>(&'a mut self, idx: uint, _size: uint) -> &'a mut T {
        self.data.get_mut(idx)
    }
}

enum ArrayParent<'parent, T> {
    SliceParent(&'parent ArraySlice<'static, T>),
    MutSliceParent(&'parent MutArraySlice<'static, T>),
    BaseParent(&'parent ArrayBase<T>),
}

/// An immutable slice of a multi-dimensional array
pub struct ArraySlice<'parent, T> {
    parent: ArrayParent<'parent, T>,
    idx: uint,
}

impl<'parent, T> Array<T> for ArraySlice<'parent, T> {
    fn dimension_info<'a>(&'a self) -> &'a [DimensionInfo] {
        let info = match self.parent {
            SliceParent(p) => p.dimension_info(),
            MutSliceParent(p) => p.dimension_info(),
            BaseParent(p) => p.dimension_info()
        };
        info.slice_from(1)
    }

    fn slice<'a>(&'a self, idx: int) -> ArraySlice<'a, T> {
        assert!(self.dimension_info().len() != 1,
                "Attempted to slice a one-dimensional array");
        unsafe {
            ArraySlice {
                parent: SliceParent(mem::transmute(self)),
                idx: self.shift_idx(idx),
            }
        }
    }

    fn get<'a>(&'a self, idx: int) -> &'a T {
        assert!(self.dimension_info().len() == 1,
                "Attempted to get from a multi-dimensional array");
        self.raw_get(self.shift_idx(idx), 1)
    }
}

impl<'parent, T> InternalArray<T> for ArraySlice<'parent, T> {
    fn raw_get<'a>(&'a self, idx: uint, size: uint) -> &'a T {
        let size = size * self.dimension_info()[0].len;
        let idx = size * self.idx + idx;
        match self.parent {
            SliceParent(p) => p.raw_get(idx, size),
            MutSliceParent(p) => p.raw_get(idx, size),
            BaseParent(p) => p.raw_get(idx, size)
        }
    }
}

enum MutArrayParent<'parent, T> {
    MutSliceMutParent(&'parent mut MutArraySlice<'static, T>),
    MutBaseParent(&'parent mut ArrayBase<T>),
}

/// A mutable slice of a multi-dimensional array
pub struct MutArraySlice<'parent, T> {
    parent: MutArrayParent<'parent, T>,
    idx: uint,
}

impl<'parent, T> Array<T> for MutArraySlice<'parent, T> {
    fn dimension_info<'a>(&'a self) -> &'a [DimensionInfo] {
        let info : &'a [DimensionInfo] = unsafe {
            match self.parent {
                MutSliceMutParent(ref p) => mem::transmute(p.dimension_info()),
                MutBaseParent(ref p) => mem::transmute(p.dimension_info()),
            }
        };
        info.slice_from(1)
    }

    fn slice<'a>(&'a self, idx: int) -> ArraySlice<'a, T> {
        assert!(self.dimension_info().len() != 1,
                "Attempted to slice a one-dimensional array");
        unsafe {
            ArraySlice {
                parent: MutSliceParent(mem::transmute(self)),
                idx: self.shift_idx(idx),
            }
        }
    }

    fn get<'a>(&'a self, idx: int) -> &'a T {
        assert!(self.dimension_info().len() == 1,
                "Attempted to get from a multi-dimensional array");
        self.raw_get(self.shift_idx(idx), 1)
    }
}

impl<'parent, T> MutableArray<T> for MutArraySlice<'parent, T> {
    fn slice_mut<'a>(&'a mut self, idx: int) -> MutArraySlice<'a, T> {
        assert!(self.dimension_info().len() != 1,
                "Attempted to slice_mut a one-dimensional array");
        unsafe {
            MutArraySlice {
                idx: self.shift_idx(idx),
                parent: MutSliceMutParent(mem::transmute(self)),
            }
        }
    }

    fn get_mut<'a>(&'a mut self, idx: int) -> &'a mut T {
        assert!(self.dimension_info().len() == 1,
                "Attempted to get_mut from a multi-dimensional array");
        let idx = self.shift_idx(idx);
        self.raw_get_mut(idx, 1)
    }
}

impl<'parent, T> InternalArray<T> for MutArraySlice<'parent, T> {
    fn raw_get<'a>(&'a self, idx: uint, size: uint) -> &'a T {
        let size = size * self.dimension_info()[0].len;
        let idx = size * self.idx + idx;
        unsafe {
            match self.parent {
                MutSliceMutParent(ref p) => mem::transmute(p.raw_get(idx, size)),
                MutBaseParent(ref p) => mem::transmute(p.raw_get(idx, size))
            }
        }
    }
}

impl<'parent, T> InternalMutableArray<T> for MutArraySlice<'parent, T> {
    fn raw_get_mut<'a>(&'a mut self, idx: uint, size: uint) -> &'a mut T {
        let size = size * self.dimension_info()[0].len;
        let idx = size * self.idx + idx;
        unsafe {
            match self.parent {
                MutSliceMutParent(ref mut p) => mem::transmute(p.raw_get_mut(idx, size)),
                MutBaseParent(ref mut p) => mem::transmute(p.raw_get_mut(idx, size))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DimensionInfo, ArrayBase, Array, MutableArray};

    #[test]
    fn test_from_vec() {
        let a = ArrayBase::from_vec(vec!(0i, 1, 2), -1);
        assert!([DimensionInfo { len: 3, lower_bound: -1 }] ==
                a.dimension_info());
        assert_eq!(&0, a.get(-1));
        assert_eq!(&1, a.get(0));
        assert_eq!(&2, a.get(1));
    }

    #[test]
    #[should_fail]
    fn test_get_2d_fail() {
        let mut a = ArrayBase::from_vec(vec!(0, 1, 2), -1);
        a.wrap(1);
        a.get(1);
    }

    #[test]
    #[should_fail]
    fn test_2d_slice_range_fail_low() {
        let mut a = ArrayBase::from_vec(vec!(0, 1, 2), -1);
        a.wrap(1);
        a.slice(0);
    }

    #[test]
    #[should_fail]
    fn test_2d_slice_range_fail_high() {
        let mut a = ArrayBase::from_vec(vec!(0, 1, 2), -1);
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
        let mut a = ArrayBase::from_vec(vec!(1), -1);
        a.push_move(ArrayBase::from_vec(vec!(2), 0));
    }

    #[test]
    #[should_fail]
    fn test_push_move_wrong_dims() {
        let mut a = ArrayBase::from_vec(vec!(1), -1);
        a.wrap(1);
        a.push_move(ArrayBase::from_vec(vec!(1, 2), -1));
    }

    #[test]
    #[should_fail]
    fn test_push_move_wrong_dim_count() {
        let mut a = ArrayBase::from_vec(vec!(1), -1);
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
        let a = ArrayBase::from_vec(vec!(1), 0);
        a.slice(0);
    }

    #[test]
    #[should_fail]
    fn test_slice_overslice() {
        let mut a = ArrayBase::from_vec(vec!(1), 0);
        a.wrap(0);
        let s = a.slice(0);
        s.slice(0);
    }
}
