use crate::hash::U256;

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
