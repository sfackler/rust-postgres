//! Multi-dimensional arrays with per-dimension specifiable lower bounds

use std::mem;
use std::slice;

/// Information about a dimension of an array
#[deriving(PartialEq, Eq, Clone)]
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
    /// ## Failure
    ///
    /// Fails if the array is one-dimensional or the index is out of bounds.
    fn slice<'a>(&'a self, idx: int) -> ArraySlice<'a, T>;

    /// Retrieves an immutable reference to a value in this array.
    ///
    ///
    /// ## Failure
    ///
    /// Fails if the array is multi-dimensional or the index is out of bounds.
    fn get<'a>(&'a self, idx: int) -> &'a T;
}

/// Specifies methods that can be performed on mutable multi-dimensional arrays
pub trait MutableArray<T> : Array<T> {
    /// Slices into this array, returning a mutable view of a subarray.
    ///
    /// ## Failure
    ///
    /// Fails if the array is one-dimensional or the index is out of bounds.
    fn slice_mut<'a>(&'a mut self, idx: int) -> MutArraySlice<'a, T>;

    /// Retrieves a mutable reference to a value in this array.
    ///
    ///
    /// ## Failure
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
    /// ## Failure
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
        self.info.insert(0, DimensionInfo {
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
    /// ## Failure
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
        &self.data[idx]
    }
}

impl<T> InternalMutableArray<T> for ArrayBase<T> {
    fn raw_get_mut<'a>(&'a mut self, idx: uint, _size: uint) -> &'a mut T {
        self.data.get_mut(idx)
    }
}

enum ArrayParent<'parent, T:'static> {
    SliceParent(&'parent ArraySlice<'static, T>),
    MutSliceParent(&'parent MutArraySlice<'static, T>),
    BaseParent(&'parent ArrayBase<T>),
}

/// An immutable slice of a multi-dimensional array
pub struct ArraySlice<'parent, T:'static> {
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

enum MutArrayParent<'parent, T:'static> {
    MutSliceMutParent(&'parent mut MutArraySlice<'static, T>),
    MutBaseParent(&'parent mut ArrayBase<T>),
}

/// A mutable slice of a multi-dimensional array
pub struct MutArraySlice<'parent, T:'static> {
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
