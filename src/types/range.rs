//! Types dealing with ranges of values
#![macro_escape]

use std::fmt;
use std::i32;
use std::i64;

/// The `range!` macro can make it easier to create ranges. It roughly mirrors
/// traditional mathematic range syntax.
///
/// ## Example
///
/// ```rust
/// #[feature(phase)];
///
/// #[phase(syntax, link)]
/// extern crate postgres;
///
/// use postgres::types::range::Range;
///
/// fn main() {
///     let mut r: Range<i32>;
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
    (empty) => (::postgres::types::range::Range::empty());
    ('(', ')') => (::postgres::types::range::Range::new(None, None));
    ('(', $h:expr ')') => (
        ::postgres::types::range::Range::new(None,
            Some(::postgres::types::range::RangeBound::new($h,
                ::postgres::types::range::Exclusive)))
    );
    ('(', $h:expr ']') => (
        ::postgres::types::range::Range::new(None,
            Some(::postgres::types::range::RangeBound::new($h,
                ::postgres::types::range::Inclusive)))
    );
    ('(' $l:expr, ')') => (
        ::postgres::types::range::Range::new(
            Some(::postgres::types::range::RangeBound::new($l,
                ::postgres::types::range::Exclusive)), None)
    );
    ('[' $l:expr, ')') => (
        ::postgres::types::range::Range::new(
            Some(::postgres::types::range::RangeBound::new($l,
                ::postgres::types::range::Inclusive)), None)
    );
    ('(' $l:expr, $h:expr ')') => (
        ::postgres::types::range::Range::new(
            Some(::postgres::types::range::RangeBound::new($l,
                ::postgres::types::range::Exclusive)),
            Some(::postgres::types::range::RangeBound::new($h,
                ::postgres::types::range::Exclusive)))
    );
    ('(' $l:expr, $h:expr ']') => (
        ::postgres::types::range::Range::new(
            Some(::postgres::types::range::RangeBound::new($l,
                ::postgres::types::range::Exclusive)),
            Some(::postgres::types::range::RangeBound::new($h,
                ::postgres::types::range::Inclusive)))
    );
    ('[' $l:expr, $h:expr ')') => (
        ::postgres::types::range::Range::new(
            Some(::postgres::types::range::RangeBound::new($l,
                ::postgres::types::range::Inclusive)),
            Some(::postgres::types::range::RangeBound::new($h,
                ::postgres::types::range::Exclusive)))
    );
    ('[' $l:expr, $h:expr ']') => (
        ::postgres::types::range::Range::new(
            Some(::postgres::types::range::RangeBound::new($l,
                ::postgres::types::range::Inclusive)),
            Some(::postgres::types::range::RangeBound::new($h,
                ::postgres::types::range::Inclusive)))
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
    fn normalize<S>(bound: RangeBound<S, Self>) -> RangeBound<S, Self> where S: BoundSided;
}

macro_rules! bounded_normalizable(
    ($t:ident) => (
        impl Normalizable for $t {
            fn normalize<S: BoundSided>(bound: RangeBound<S, $t>) -> RangeBound<S, $t> {
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

/// The possible sides of a bound
#[deriving(PartialEq, Eq)]
pub enum BoundSide {
    /// An upper bound
    Upper,
    /// A lower bound
    Lower
}

/// A trait implemented by phantom types indicating the type of the bound
#[doc(hidden)]
pub trait BoundSided {
    /// Returns the bound side this type corresponds to
    // param is a hack to get around lack of hints for self type
    fn side(_: Option<Self>) -> BoundSide;
}

/// A tag type representing an upper bound
pub enum UpperBound {}

/// A tag type representing a lower bound
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
#[deriving(PartialEq, Eq, Clone)]
pub enum BoundType {
    /// The bound includes its value
    Inclusive,
    /// The bound excludes its value
    Exclusive
}

/// Represents a one-sided bound.
///
/// The side is determined by the `S` phantom parameter.
pub struct RangeBound<S: BoundSided, T> {
    /// The value of the bound
    pub value: T,
    /// The type of the bound
    pub type_: BoundType
}

impl<S, T> Clone for RangeBound<S, T> where S: BoundSided, T: Clone {
    fn clone(&self) -> RangeBound<S, T> {
        RangeBound {
            value: self.value.clone(),
            type_: self.type_.clone(),
        }
    }
}

impl<S, T> fmt::Show for RangeBound<S, T> where S: BoundSided, T: fmt::Show {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let (lower, upper) = match self.type_ {
            Inclusive => ('[', ']'),
            Exclusive => ('(', ')'),
        };

        match BoundSided::side(None::<S>) {
            Lower => write!(fmt, "{}{}", lower, self.value),
            Upper => write!(fmt, "{}{}", self.value, upper),
        }
    }
}

impl<S, T> PartialEq for RangeBound<S, T> where S: BoundSided, T: PartialEq {
    fn eq(&self, other: &RangeBound<S, T>) -> bool {
        self.value == other.value && self.type_ == other.type_
    }

    fn ne(&self, other: &RangeBound<S, T>) -> bool {
        self.value != other.value || self.type_ != other.type_
    }
}

impl<S, T> Eq for RangeBound<S, T> where S: BoundSided, T: Eq {}

impl<S, T> PartialOrd for RangeBound<S, T> where S: BoundSided, T: PartialOrd {
    fn partial_cmp(&self, other: &RangeBound<S, T>) -> Option<Ordering> {
        match (BoundSided::side(None::<S>), self.type_, other.type_,
                self.value.partial_cmp(&other.value)) {
            (Upper, Exclusive, Inclusive, Some(Equal))
            | (Lower, Inclusive, Exclusive, Some(Equal)) => Some(Less),
            (Upper, Inclusive, Exclusive, Some(Equal))
            | (Lower, Exclusive, Inclusive, Some(Equal)) => Some(Greater),
            (_, _, _, cmp) => cmp,
        }
    }
}

impl<S, T> Ord for RangeBound<S, T> where S: BoundSided, T: Ord {
    fn cmp(&self, other: &RangeBound<S, T>) -> Ordering {
        match (BoundSided::side(None::<S>), self.type_, other.type_,
                self.value.cmp(&other.value)) {
            (Upper, Exclusive, Inclusive, Equal)
            | (Lower, Inclusive, Exclusive, Equal) => Less,
            (Upper, Inclusive, Exclusive, Equal)
            | (Lower, Exclusive, Inclusive, Equal) => Greater,
            (_, _, _, ord) => ord,
        }
    }
}

impl<S, T> RangeBound<S, T> where S: BoundSided, T: PartialOrd {
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

struct OptBound<'a, S: BoundSided, T:'a>(Option<&'a RangeBound<S, T>>);

impl<'a, S, T> PartialEq for OptBound<'a, S, T> where S: BoundSided, T: PartialEq {
    fn eq(&self, &OptBound(ref other): &OptBound<'a, S, T>) -> bool {
        let &OptBound(ref self_) = self;
        self_ == other
    }

    fn ne(&self, &OptBound(ref other): &OptBound<'a, S, T>) -> bool {
        let &OptBound(ref self_) = self;
        self_ != other
    }
}

impl<'a, S, T> PartialOrd for OptBound<'a, S, T> where S: BoundSided, T: PartialOrd {
    fn partial_cmp(&self, other: &OptBound<'a, S, T>) -> Option<Ordering> {
        match (*self, *other, BoundSided::side(None::<S>)) {
            (OptBound(None), OptBound(None), _) => Some(Equal),
            (OptBound(None), _, Lower)
            | (_, OptBound(None), Upper) => Some(Less),
            (OptBound(None), _, Upper)
            | (_, OptBound(None), Lower) => Some(Greater),
            (OptBound(Some(a)), OptBound(Some(b)), _) => a.partial_cmp(b)
        }
    }
}

/// Represents a range of values.
#[deriving(PartialEq, Eq, Clone)]
pub struct Range<T> {
    inner: InnerRange<T>,
}

#[deriving(PartialEq, Eq, Clone)]
enum InnerRange<T> {
    Empty,
    Normal(Option<RangeBound<LowerBound, T>>,
           Option<RangeBound<UpperBound, T>>)
}

impl<T> fmt::Show for Range<T> where T: fmt::Show {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self.inner {
            Empty => write!(fmt, "empty"),
            Normal(ref lower, ref upper) => {
                match *lower {
                    Some(ref bound) => try!(write!(fmt, "{}", bound)),
                    None => try!(write!(fmt, "(")),
                }
                try!(write!(fmt, ","));
                match *upper {
                    Some(ref bound) => write!(fmt, "{}", bound),
                    None => write!(fmt, ")"),
                }
            }
        }
    }
}

impl<T> Range<T> where T: PartialOrd+Normalizable{
    /// Creates a new range.
    ///
    /// If a bound is `None`, the range is unbounded in that direction.
    pub fn new(lower: Option<RangeBound<LowerBound, T>>,
               upper: Option<RangeBound<UpperBound, T>>) -> Range<T> {
        let lower = lower.map(Normalizable::normalize);
        let upper = upper.map(Normalizable::normalize);

        if let (&Some(ref lower), &Some(ref upper)) = (&lower, &upper) {
            let empty = match (lower.type_, upper.type_) {
                (Inclusive, Inclusive) => lower.value > upper.value,
                _ => lower.value >= upper.value
            };
            if empty {
                return Range { inner: Empty };
            }
        }

        Range { inner: Normal(lower, upper) }
    }

    /// Creates a new empty range.
    pub fn empty() -> Range<T> {
        Range { inner: Empty }
    }

    /// Determines if this range is the empty range.
    pub fn is_empty(&self) -> bool {
        match self.inner {
            Empty => true,
            Normal(..) => false
        }
    }

    /// Returns the lower bound if it exists.
    pub fn lower(&self) -> Option<&RangeBound<LowerBound, T>> {
        match self.inner {
            Normal(Some(ref lower), _) => Some(lower),
            _ => None
        }
    }

    /// Returns the upper bound if it exists.
    pub fn upper(&self) -> Option<&RangeBound<UpperBound, T>> {
        match self.inner {
            Normal(_, Some(ref upper)) => Some(upper),
            _ => None
        }
    }

    /// Determines if a value lies within this range.
    pub fn contains(&self, value: &T) -> bool {
        match self.inner {
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

fn order<T>(a: T, b: T) -> (T, T) where T: PartialOrd {
    if a < b {
        (a, b)
    } else {
        (b, a)
    }
}

impl<T> Range<T> where T: PartialOrd+Normalizable+Clone {
    /// Returns the intersection of this range with another
    pub fn intersect(&self, other: &Range<T>) -> Range<T> {
        if self.is_empty() || other.is_empty() {
            return Range::empty();
        }

        let (_, OptBound(lower)) = order(OptBound(self.lower()),
                                         OptBound(other.lower()));
        let (OptBound(upper), _) = order(OptBound(self.upper()),
                                         OptBound(other.upper()));

        Range::new(lower.map(|v| v.clone()), upper.map(|v| v.clone()))
    }

    /// Returns the union of this range with another if it is contiguous
    pub fn union(&self, other: &Range<T>) -> Option<Range<T>> {
        if self.is_empty() {
            return Some(other.clone());
        }

        if other.is_empty() {
            return Some(self.clone());
        }

        let (OptBound(l_lower), OptBound(u_lower)) =
            order(OptBound(self.lower()), OptBound(other.lower()));
        let (OptBound(l_upper), OptBound(u_upper)) =
            order(OptBound(self.upper()), OptBound(other.upper()));

        let discontiguous = match (u_lower, l_upper) {
            (Some(&RangeBound { value: ref l, type_: Exclusive }),
             Some(&RangeBound { value: ref u, type_: Exclusive })) => l >= u,
            (Some(&RangeBound { value: ref l, .. }),
             Some(&RangeBound { value: ref u, .. })) => l > u,
            _ => false
        };

        if discontiguous {
            None
        } else {
            Some(Range::new(l_lower.map(|v| v.clone()), u_upper.map(|v| v.clone())))
        }
    }
}
