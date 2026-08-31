use sha256::digest;
use std::net::{SocketAddr, UdpSocket};
use uint::construct_uint;

use crate::ChunkSended;

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

pub struct Chunk<'a> {
    pub id: U256,
    pub msg: &'a [u8],
    pub index: u16,
    pub total_chunks: u16,
}

impl<'a> Chunk<'a> {
    // CONVERT GIVEN CHUCNK TO BYTES
    pub fn to_byte(&self) -> Vec<u8> {
        let byte_size = 32 + 4 + self.msg.len() + 2 + 2; // 32 (Hash(msg_id)) + 4 (msg_len) + msg + 2 (index) + 2 (total_chunk_size)
        let mut buffer = Vec::with_capacity(byte_size);

        buffer.extend_from_slice(&self.id.to_little_endian());

        let msg_len = self.msg.len() as u32;
        buffer.extend_from_slice(&msg_len.to_le_bytes());

        buffer.extend_from_slice(&self.msg);
        buffer.extend_from_slice(&self.index.to_le_bytes());
        buffer.extend_from_slice(&self.total_chunks.to_le_bytes());

        buffer
    }

    pub fn from_byte(buffer: &'a [u8]) -> Result<Self, &'static str> {
        // 1. Min header size: 32 (Hash) + 4 (msg_len) + 2 (index) + 2 (total_chunks) = 40 bytes
        const MIN_HEADER_SIZE: usize = 40;
        if buffer.len() < MIN_HEADER_SIZE {
            return Err("Buffer is too small for a valid message chunk");
        }

        // 2. Message ID / Hash
        let mut id_byte = [0u8; 32];
        id_byte.copy_from_slice(&buffer[0..32]);
        let id = U256::from_little_endian(&id_byte);

        // 3. Message Payload Length
        let mut msg_len_byte = [0u8; 4];
        msg_len_byte.copy_from_slice(&buffer[32..36]);
        let msg_len = u32::from_le_bytes(msg_len_byte) as usize;

        if buffer.len() < 36 + msg_len + 4 {
            return Err("Buffer payload is truncated or corrupted");
        }

        // 5. Payload
        let msg = &buffer[36..36 + msg_len];

        // 6. Chunk Index (2 bytes after the payload)
        let index_offset = 36 + msg_len;
        let mut index_byte = [0u8; 2];
        index_byte.copy_from_slice(&buffer[index_offset..index_offset + 2]);
        let index = u16::from_le_bytes(index_byte);

        // 7. Total Chunks count (2 bytes after the index)
        let total_offset = index_offset + 2;
        let mut total_byte = [0u8; 2];
        total_byte.copy_from_slice(&buffer[total_offset..total_offset + 2]);
        let total_chunks = u16::from_le_bytes(total_byte);

        if total_chunks == 0 || index >= total_chunks {
            return Err("Invalid chunk index or total count");
        }

        Ok(Chunk {
            id,
            msg,
            index,
            total_chunks,
        })
    }
}

// CREATE CHUNK AND THEN SEND TO ALL PEERS
impl ChunkSended {
    pub fn send_in_chunks(
        &mut self,
        msg: &str,
        msg_id: Hash,
        peers: Vec<SocketAddr>,
        socket: &UdpSocket,
    ) {
        let msg_byte = msg.as_bytes();
        const MAX_SAFE_PAYLOAD: usize = 1400;

        let all_packet: Vec<(usize, &[u8])> =
            msg_byte.chunks(MAX_SAFE_PAYLOAD).enumerate().collect();

        let total_chunk = all_packet.len() as u16;

        for chunk_batch in all_packet.chunks(25) {
            // Ready to send batch with ecrypton
            let mut batch = Vec::with_capacity(chunk_batch.len());

            for (index, chunk_slice) in chunk_batch {
                let chunk = Chunk {
                    id: msg_id.0.clone(),
                    msg: chunk_slice,
                    index: *index as u16,
                    total_chunks: total_chunk,
                };

                // TODO: after converting to bytes encrypt it
                let chunk_byte = chunk.to_byte();

                self.0
                    .entry(msg_id.0.clone())
                    .or_default()
                    .insert(*index as u16, chunk_byte.clone());

                batch.push(chunk_byte);
            }

            for packet in batch {
                for peer_list in peers.chunks(5) {
                    for peer in peer_list {
                        socket.send_to(&packet, peer).expect("Failed to send");
                    }
                }
            }
        }
    }
}
