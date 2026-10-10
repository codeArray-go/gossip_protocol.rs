use crate::{Combined, U256, byte_converter::Serializer};
use sha256::digest;

#[derive(Debug, Clone, Copy)]
pub struct Hash(pub U256);

impl Hash {
    pub fn of<T: Serializer>(item: &T) -> Self {
        let mut serialize: Vec<u8> = vec![];
        item.serialize(&mut serialize);

        let hash = digest(serialize);
        let hash_array: [u8; 32] = hex::decode(hash).unwrap().as_slice().try_into().unwrap();
        Hash(U256::from_little_endian(&hash_array))
    }
}

pub fn merkle_root(data: &[Combined]) -> Hash {
    let mut layer: Vec<Hash> = vec![];
    for val in data {
        layer.push(Hash::of(val))
    }

    while layer.len() > 1 {
        let mut new_layer: Vec<Hash> = vec![];

        for pair in layer.chunks(2) {
            let left = &pair[0];
            let right = pair.get(1).unwrap_or(&pair[0]);
            new_layer.push(Hash::of(&[left.0, right.0]));
        }

        layer = new_layer;
    }

    layer[0]
}
