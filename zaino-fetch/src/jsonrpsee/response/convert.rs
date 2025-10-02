// src/convert.rs (or similar)

/// Converts a source type into an internal domain type.
pub trait IntoInternal<T> {
    /// Convert to the internal representation.
    fn into_internal(self) -> T;
}

/// Fallible version for conversions that can fail.
pub trait TryIntoInternal<T> {
    type Error;
    /// Try to convert to the internal representation.
    fn try_into_internal(self) -> Result<T, Self::Error>;
}

impl<T, U> IntoInternal<U> for T
where
    U: From<T>,
{
    #[inline]
    fn into_internal(self) -> U {
        U::from(self)
    }
}

impl<T, U> TryIntoInternal<U> for T
where
    U: core::convert::TryFrom<T>,
{
    type Error = <U as core::convert::TryFrom<T>>::Error;

    #[inline]
    fn try_into_internal(self) -> Result<U, Self::Error> {
        U::try_from(self)
    }
}
