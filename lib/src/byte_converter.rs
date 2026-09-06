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

// DESERIALIZER
pub trait Deserializer<'de>: Sized {
    fn deserialze(buffer: &mut &'de [u8]) -> Result<Self, &'static str>;
}

impl<'de> Deserializer<'de> for u16 {
    fn deserialze(buffer: &mut &'de [u8]) -> Result<Self, &'static str> {
        if buffer.len() < 2 {
            return Err("Too small for u16 value");
        }

        let (bytes, rest) = buffer.split_at(2);
        *buffer = rest;
        Ok(u16::from_le_bytes(bytes.try_into().unwrap()))
    }
}

impl<'de> Deserializer<'de> for u32 {
    fn deserialze(buffer: &mut &'de [u8]) -> Result<Self, &'static str> {
        if buffer.len() < 4 {
            return Err("Too small for u32 value");
        }

        let (bytes, rest) = buffer.split_at(4);
        *buffer = rest;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }
}

impl<'de> Deserializer<'de> for String {
    fn deserialze(buffer: &mut &'de [u8]) -> Result<Self, &'static str> {
        let len = u32::deserialze(buffer)? as usize;
        if buffer.len() < len {
            return Err("Too small for a String value");
        }

        let (bytes, rest) = buffer.split_at(len);
        *buffer = rest;

        let string_slice = std::str::from_utf8(bytes).map_err(|_| "Invalid UTF-8 squence")?;

        Ok(string_slice.to_string())
    }
}
