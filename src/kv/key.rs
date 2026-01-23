// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::ops::Bound;

#[allow(unused_imports)]
#[cfg(test)]
use proptest::arbitrary::any_with;
#[allow(unused_imports)]
#[cfg(test)]
use proptest::collection::size_range;
#[cfg(test)]
use proptest_derive::Arbitrary;

use super::HexRepr;
use crate::kv::codec::BytesEncoder;
use crate::kv::codec::{self};
use crate::proto::kvrpcpb;
use crate::proto::kvrpcpb::KvPair;

const _PROPTEST_KEY_MAX: usize = 1024 * 2; // 2 KB

/// The key part of a key/value pair.
///
/// In TiKV, keys are an ordered sequence of bytes. This has an advantage over choosing `String` as
/// valid `UTF-8` is not required. This means that the user is permitted to store any data they wish,
/// as long as it can be represented by bytes. (Which is to say, pretty much anything!)
///
/// This type wraps around an owned value, so it should be treated it like `String` or `Vec<u8>`.
///
/// # Examples
/// ```rust
/// use tikv_client::Key;
///
/// let static_str: &'static str = "TiKV";
/// let from_static_str = Key::from(static_str.to_owned());
///
/// let string: String = String::from(static_str);
/// let from_string = Key::from(string);
/// assert_eq!(from_static_str, from_string);
///
/// let vec: Vec<u8> = static_str.as_bytes().to_vec();
/// let from_vec = Key::from(vec);
/// assert_eq!(from_static_str, from_vec);
///
/// let bytes = static_str.as_bytes().to_vec();
/// let from_bytes = Key::from(bytes);
/// assert_eq!(from_static_str, from_bytes);
/// ```
///
/// While `.into()` is usually sufficient for obtaining the buffer itself, sometimes type inference
/// isn't able to determine the correct type. Notably in the `assert_eq!()` and `==` cases. In
/// these cases using the fully-qualified-syntax is useful:
///
/// # Examples
/// ```rust
/// use tikv_client::Key;
///
/// let buf = "TiKV".as_bytes().to_owned();
/// let key = Key::from(buf.clone());
/// assert_eq!(Into::<Vec<u8>>::into(key), buf);
/// ```
///
/// Many functions which accept a `Key` accept an `Into<Key>`, which means all of the above types
/// can be passed directly to those functions.
#[derive(Default, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(test, derive(Arbitrary))]
#[repr(transparent)]
pub struct Key(
    #[cfg_attr(
        test,
        proptest(strategy = "any_with::<Vec<u8>>((size_range(_PROPTEST_KEY_MAX), ()))")
    )]
    pub(crate) Vec<u8>,
);

impl AsRef<Key> for kvrpcpb::Mutation {
    fn as_ref(&self) -> &Key {
        self.key.as_ref()
    }
}

pub struct KvPairTTL(pub KvPair, pub u64);

impl AsRef<Key> for KvPairTTL {
    fn as_ref(&self) -> &Key {
        self.0.key.as_ref()
    }
}

impl From<KvPairTTL> for (KvPair, u64) {
    fn from(value: KvPairTTL) -> Self {
        (value.0, value.1)
    }
}

impl Key {
    /// The empty key.
    pub const EMPTY: Self = Key(Vec::new());

    /// Return whether the key is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Return whether the last byte of key is 0.
    #[inline]
    pub(super) fn zero_terminated(&self) -> bool {
        self.0.last().map(|i| *i == 0).unwrap_or(false)
    }

    /// Push a zero to the end of the key.
    ///
    /// Extending a zero makes the new key the smallest key that is greater than than the original one.
    #[inline]
    pub(crate) fn next_key(mut self) -> Self {
        self.0.push(0);
        self
    }

    /// Convert the key to a lower bound. The key is treated as inclusive.
    #[inline]
    pub(super) fn into_lower_bound(mut self) -> Bound<Key> {
        if self.zero_terminated() {
            // `zero_terminated` guarantees there is a last byte.
            let _ = self.0.pop();
            Bound::Excluded(self)
        } else {
            Bound::Included(self)
        }
    }

    /// Convert the key to an upper bound. The key is treated as exclusive.
    #[inline]
    pub(super) fn into_upper_bound(mut self) -> Bound<Key> {
        if self.zero_terminated() {
            // `zero_terminated` guarantees there is a last byte.
            let _ = self.0.pop();
            Bound::Included(self)
        } else {
            Bound::Excluded(self)
        }
    }

    /// Return the MVCC-encoded representation of the key.
    #[inline]
    #[must_use]
    pub fn to_encoded(&self) -> Key {
        let len = codec::max_encoded_bytes_size(self.0.len());
        let mut encoded = Vec::with_capacity(len);
        // Encoding into `Vec<u8>` is infallible. Keep `to_encoded` non-panicking by construction.
        let _ = encoded.encode_bytes(&self.0, false);
        Key(encoded)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Return the next prefix key in byte-order.
    ///
    /// This matches client-go's `kv.PrefixNextKey` semantics:
    /// - increment the key as a big-endian byte slice (carry propagation),
    /// - if the key is all `0xff`, return the empty key.
    // Older toolchains used by TiKV don't support `#[expect(..., reason = "...")]`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn next_prefix_key(&self) -> Key {
        let mut buf = self.0.clone();
        for i in (0..buf.len()).rev() {
            buf[i] = buf[i].wrapping_add(1);
            if buf[i] != 0 {
                return Key(buf);
            }
        }
        Key::EMPTY
    }
}

impl From<Vec<u8>> for Key {
    fn from(v: Vec<u8>) -> Self {
        Key(v)
    }
}

impl From<String> for Key {
    fn from(v: String) -> Key {
        Key(v.into_bytes())
    }
}

impl From<Key> for Vec<u8> {
    fn from(key: Key) -> Self {
        key.0
    }
}

impl<'a> From<&'a Key> for &'a [u8] {
    fn from(key: &'a Key) -> Self {
        &key.0
    }
}

impl<'a> From<&'a Vec<u8>> for &'a Key {
    fn from(key: &'a Vec<u8>) -> Self {
        // SAFETY: `Key` is `#[repr(transparent)]` over `Vec<u8>`, so the layout is identical.
        // We only create a shared reference with the same lifetime as the source reference.
        unsafe { &*(key as *const Vec<u8> as *const Key) }
    }
}
impl AsRef<Key> for Key {
    fn as_ref(&self) -> &Key {
        self
    }
}

impl AsRef<Key> for Vec<u8> {
    fn as_ref(&self) -> &Key {
        // SAFETY: `Key` is `#[repr(transparent)]` over `Vec<u8>`, so the layout is identical.
        // We only create a shared reference with the same lifetime as the source reference.
        unsafe { &*(self as *const Vec<u8> as *const Key) }
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Key({})", HexRepr(&self.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vec_key_ref_cast_round_trips_bytes() {
        let v = vec![0xAA, 0xBB, 0xCC];
        let k: &Key = (&v).into();
        let bytes: &[u8] = k.into();
        assert_eq!(bytes, v.as_slice());

        let k2: &Key = v.as_ref();
        let bytes2: &[u8] = k2.into();
        assert_eq!(bytes2, v.as_slice());
    }

    #[test]
    fn next_prefix_key_matches_client_go_key_test() {
        let k1 = Key::from(vec![0xff]);
        let k2 = Key::from(vec![0xff, 0xff]);
        let k3 = Key::from(vec![0xff, 0xff, 0xff, 0xff]);

        assert_eq!(k1.next_prefix_key(), Key::EMPTY);
        assert_eq!(k2.next_prefix_key(), Key::EMPTY);
        assert_eq!(k3.next_prefix_key(), Key::EMPTY);

        let k = Key::from(vec![0x01, 0x02]);
        assert_eq!(k.next_prefix_key(), Key::from(vec![0x01, 0x03]));

        let k = Key::from(vec![0x01, 0xff]);
        assert_eq!(k.next_prefix_key(), Key::from(vec![0x02, 0x00]));
    }
}
