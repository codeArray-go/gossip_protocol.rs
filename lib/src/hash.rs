use sha256::digest;
use uint::construct_uint;

construct_uint!(
    pub struct U256(4);
);

#[derive(Debug, Clone)]
pub struct Hash(pub U256);

impl Hash {
    pub fn of<T: serde::Serialize>(item: &T) -> Self {
        let mut serialize: Vec<u8> = vec![];
        if let Err(e) = ciborium::into_writer(item, &mut serialize) {
            panic!(
                "Failed to serialise data: {:?} \
            This shouldn't have to happen.",
                e
            );
        }

        let hash = digest(serialize);
        let hash_array: [u8; 32] = hex::decode(hash).unwrap().as_slice().try_into().unwrap();
        Hash(U256::from_little_endian(&hash_array))
    }
}
