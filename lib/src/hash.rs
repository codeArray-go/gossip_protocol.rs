use crate::{U256, byte_converter::Serializer};
use sha256::digest;

#[derive(Debug, Clone)]
pub struct Hash(pub U256);

impl Hash {
    pub fn of<T: Serializer>(item: &T) -> Self {
        let mut serialize: Vec<u8> = vec![];
        Serializer::serialize(item, &mut serialize);

        let hash = digest(serialize);
        let hash_array: [u8; 32] = hex::decode(hash).unwrap().as_slice().try_into().unwrap();
        Hash(U256::from_little_endian(&hash_array))
    }
}
