use serde::{ Deserialize, Serialize };
use uint::construct_uint;
use sha256::digest;

construct_uint! {
    #[derive(Serialize, Deserialize)]
    pub struct U256(4);
}

#[derive(Serialize, Deserialize)]
pub struct Hash(pub U256);

impl Hash {
    pub fn from<T: serde::Serialize>(data: &T) -> Self {
        let mut serialize: Vec<u8> = vec![];
        if let Err(e) = ciborium::into_writer(data, &mut serialize) {
            panic!("Failed to serialize data: {:?} \
            This shouldn't happen", e);
        }

        let hash = digest(serialize);
        let hash_bytes = hex::decode(hash).unwrap();
        let hash_array: [u8; 32] = hash_bytes.as_slice().try_into().unwrap();
        Hash(U256::from_little_endian(&hash_array))
    }
}
