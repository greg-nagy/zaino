//! Holds the Finalised portion of the chain index on disk.

use blake2::{
    digest::{Update, VariableOutput},
    Blake2bVar,
};
use dcbor::{CBORCase, CBORTagged, CBORTaggedDecodable, CBORTaggedEncodable, Tag, CBOR};

/// DCBOR serialisation schema tags.
///
/// All structs in this module should be included in this enum.
///
/// Items should use the range 500 - 509 (510 - 599 are reserved for chain structs).
#[repr(u64)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbSchemaTag {
    StoredEntry = 500,
    DbMetadata = 501,
    DbVersion = 502,
}

impl DbSchemaTag {
    pub fn tag(self) -> dcbor::Tag {
        dcbor::Tag::with_value(self as u64)
    }
}

/// Wrapper around a CBOR-tagged item and its BLAKE2b checksum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEntry<T>
where
    T: CBORTaggedEncodable + CBORTaggedDecodable + CBORTagged,
{
    /// The inner tagged item being stored.
    pub item: T,
    /// The BLAKE2b-256 checksum of the canonical dCBOR encoding of `item`.
    pub checksum: [u8; 32],
}

impl<T> StoredEntry<T>
where
    T: CBORTaggedEncodable + CBORTaggedDecodable + CBORTagged,
{
    /// Construct a new `StoredEntry` from an item, computing its checksum.
    pub fn new(item: T) -> Self {
        let encoded = item.tagged_cbor().to_cbor_data();
        let checksum = Self::compute_blake2b_256(&encoded);
        Self { item, checksum }
    }

    /// Verifies the checksum matches the current item's encoding.
    pub fn verify(&self) -> bool {
        let encoded = self.item.tagged_cbor().to_cbor_data();
        self.checksum == Self::compute_blake2b_256(&encoded)
    }

    /// Serializes the entry into canonical dCBOR.
    pub fn serialize(&self) -> Vec<u8> {
        self.tagged_cbor().to_cbor_data()
    }

    /// Deserializes a `StoredEntry<T>` from canonical dCBOR bytes.
    pub fn deserialize(bytes: &[u8]) -> dcbor::Result<Self> {
        let cbor = CBOR::try_from_data(bytes)?;
        Self::from_tagged_cbor(cbor)
    }

    /// Computes a BLAKE2b-256 checksum.
    fn compute_blake2b_256(data: &[u8]) -> [u8; 32] {
        let mut hasher = Blake2bVar::new(32).expect("Failed to create hasher");
        hasher.update(data);
        let mut output = [0u8; 32];
        hasher
            .finalize_variable(&mut output)
            .expect("Failed to finalize hash");
        output
    }
}

impl<T> CBORTagged for StoredEntry<T>
where
    T: CBORTaggedEncodable + CBORTaggedDecodable + CBORTagged,
{
    fn cbor_tags() -> Vec<Tag> {
        vec![DbSchemaTag::StoredEntry.tag()]
    }
}

impl<T: CBORTaggedEncodable> CBORTaggedEncodable for StoredEntry<T>
where
    T: CBORTaggedEncodable + CBORTaggedDecodable + CBORTagged,
{
    fn untagged_cbor(&self) -> CBOR {
        CBOR::from(vec![
            self.item.tagged_cbor(),
            CBOR::from(&self.checksum[..]),
        ])
    }
}

impl<T: CBORTaggedDecodable> CBORTaggedDecodable for StoredEntry<T>
where
    T: CBORTaggedEncodable + CBORTaggedDecodable + CBORTagged,
{
    fn from_untagged_cbor(cbor: CBOR) -> dcbor::Result<Self> {
        match cbor.into_case() {
            CBORCase::Array(arr) if arr.len() == 2 => {
                let item = T::from_tagged_cbor(arr[0].clone())?;
                let bytes: Vec<u8> = arr[1].clone().try_into()?;
                let checksum: [u8; 32] = bytes.try_into().map_err(|_| dcbor::Error::WrongType)?;
                Ok(Self { item, checksum })
            }
            _ => Err(dcbor::Error::WrongType),
        }
    }
}

impl<T: CBORTaggedDecodable> TryFrom<CBOR> for StoredEntry<T>
where
    T: CBORTaggedEncodable + CBORTaggedDecodable + CBORTagged,
{
    type Error = dcbor::Error;

    fn try_from(cbor: CBOR) -> Result<Self, Self::Error> {
        Self::from_tagged_cbor(cbor)
    }
}

/// Top-level database metadata entry, storing the current schema version.
///
/// Stored under the fixed key `"metadata"` in the LMDB metadata database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbMetadata {
    /// Encodes the version and schema hash.
    pub version: DbVersion,
}

impl CBORTagged for DbMetadata {
    fn cbor_tags() -> Vec<dcbor::Tag> {
        vec![DbSchemaTag::DbMetadata.tag()]
    }
}

impl CBORTaggedEncodable for DbMetadata {
    fn untagged_cbor(&self) -> CBOR {
        self.version.tagged_cbor()
    }
}

impl CBORTaggedDecodable for DbMetadata {
    fn from_untagged_cbor(cbor: CBOR) -> dcbor::Result<Self> {
        Ok(Self {
            version: DbVersion::from_tagged_cbor(cbor)?,
        })
    }
}

impl TryFrom<CBOR> for DbMetadata {
    type Error = dcbor::Error;
    fn try_from(cbor: CBOR) -> dcbor::Result<Self> {
        Self::from_tagged_cbor(cbor)
    }
}

/// Database schema version information.
///
/// This is used for schema migration safety and compatibility checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbVersion {
    /// Monotonically increasing version number (e.g., 1, 2, ...)
    pub version: u32,
    /// BLAKE2b-256 hash of the schema definition (includes struct layout, types, etc.)
    pub schema_hash: [u8; 32],
}

impl CBORTagged for DbVersion {
    fn cbor_tags() -> Vec<dcbor::Tag> {
        vec![DbSchemaTag::DbVersion.tag()]
    }
}

impl CBORTaggedEncodable for DbVersion {
    fn untagged_cbor(&self) -> CBOR {
        CBOR::from(vec![
            CBOR::from(self.version),
            CBOR::from(&self.schema_hash[..]),
        ])
    }
}

impl CBORTaggedDecodable for DbVersion {
    fn from_untagged_cbor(cbor: CBOR) -> dcbor::Result<Self> {
        match cbor.into_case() {
            CBORCase::Array(arr) if arr.len() == 2 => {
                let version = arr[0].clone().try_into()?;
                let hash_vec: Vec<u8> = arr[1].clone().try_into()?;
                let schema_hash: [u8; 32] =
                    hash_vec.try_into().map_err(|_| dcbor::Error::WrongType)?;
                Ok(Self {
                    version,
                    schema_hash,
                })
            }
            _ => Err(dcbor::Error::WrongType),
        }
    }
}

impl TryFrom<CBOR> for DbVersion {
    type Error = dcbor::Error;
    fn try_from(cbor: CBOR) -> dcbor::Result<Self> {
        Self::from_tagged_cbor(cbor)
    }
}
