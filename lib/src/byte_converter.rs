use crate::U256;
use std::{collections::HashMap, hash::Hash};

// SERIALIZER
pub trait Serializer {
    fn serialize(&self, buffer: &mut Vec<u8>);
}

impl Serializer for u32 {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_le_bytes());
    }
}

impl Serializer for u8 {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        buffer.push(*self);
    }
}

impl Serializer for u16 {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_le_bytes());
    }
}

impl Serializer for String {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        let len = self.len() as u32;
        len.serialize(buffer);
        buffer.extend_from_slice(&self.as_bytes());
    }
}

impl Serializer for &[u8] {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        let len = self.len() as u32;
        len.serialize(buffer);
        buffer.extend_from_slice(self);
    }
}

impl Serializer for U256 {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_little_endian());
    }
}

impl<K, V> Serializer for HashMap<K, V>
where
    K: Serializer,
    V: Serializer,
{
    fn serialize(&self, buffer: &mut Vec<u8>) {
        let len = self.len() as u32;
        len.serialize(buffer);

        for (key, value) in self {
            key.serialize(buffer);
            value.serialize(buffer);
        }
    }
}

// DESERIALIZER
pub trait Deserializer<'a>: Sized {
    fn deserialze(buffer: &mut &'a [u8]) -> Result<Self, &'static str>;
}

impl<'a> Deserializer<'a> for u16 {
    fn deserialze(buffer: &mut &'a [u8]) -> Result<Self, &'static str> {
        if buffer.len() < 2 {
            return Err("Too small for u16 value");
        }

        let (bytes, rest) = buffer.split_at(2);
        *buffer = rest;
        Ok(u16::from_le_bytes(bytes.try_into().unwrap()))
    }
}

impl<'a> Deserializer<'a> for u32 {
    fn deserialze(buffer: &mut &'a [u8]) -> Result<Self, &'static str> {
        if buffer.len() < 4 {
            return Err("Too small for u32 value");
        }

        let (bytes, rest) = buffer.split_at(4);
        *buffer = rest;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }
}

impl<'a> Deserializer<'a> for String {
    fn deserialze(buffer: &mut &'a [u8]) -> Result<Self, &'static str> {
        let len = u32::deserialze(buffer)? as usize;
        if buffer.len() < len {
            return Err("Too small for a String value");
        }

        let (bytes, rest) = buffer.split_at(len);
        *buffer = rest;

        let string_slice = std::str::from_utf8(bytes).map_err(|_| "Invalid UTF-8 sequence")?;

        Ok(string_slice.to_string())
    }
}

impl<'a> Deserializer<'a> for U256 {
    fn deserialze(buffer: &mut &'a [u8]) -> Result<Self, &'static str> {
        if buffer.len() < 32 {
            return Err("Too small for a U256 value");
        }

        let (bytes, rest) = buffer.split_at(32);
        *buffer = rest;
        Ok(U256::from_little_endian(bytes))
    }
}

impl<'a, K, V> Deserializer<'a> for HashMap<K, V>
where
    K: Deserializer<'a> + Eq + Hash,
    V: Deserializer<'a>,
{
    fn deserialze(buffer: &mut &'a [u8]) -> Result<Self, &'static str> {
        let len = u32::deserialze(buffer)? as usize;

        let mut map = HashMap::with_capacity(len);

        for _ in 0..len {
            let key = K::deserialze(buffer)?;
            let value = V::deserialze(buffer)?;

            map.insert(key, value);
        }

        Ok(map)
    }
}
