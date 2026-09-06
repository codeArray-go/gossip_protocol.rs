use crate::ChunkSended;
use p2p_lib::{
    byte_converter::{Deserializer, Serializer},
    hash::{Hash, U256},
};
use std::{
    io::Result,
    net::{SocketAddr, UdpSocket},
};

pub struct Chunk<'a> {
    pub id: U256,
    pub msg: &'a [u8],
    pub index: u16,
    pub total_chunks: u16,
}

impl<'a> Serializer for Chunk<'a> {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        self.id.serialize(buffer);
        self.index.serialize(buffer);
        self.msg.serialize(buffer);
        self.index.serialize(buffer);
        self.total_chunks.serialize(buffer);
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
            let mut batch = Vec::with_capacity(chunk_batch.len());

            for (index, chunk_slice) in chunk_batch {
                let chunk = Chunk {
                    id: msg_id.0.clone(),
                    msg: chunk_slice,
                    index: *index as u16,
                    total_chunks: total_chunk,
                };

                // 32 (Hash(msg_id)) + 4 (msg_len) + msg + 2 (index) + 2 (total_chunk_size)
                let byte_size = 32 + 4 + chunk_slice.len() + 2 + 2;
                let mut buffer = Vec::with_capacity(byte_size);
                chunk.serialize(&mut buffer);

                // TODO: after converting to bytes encrypt it

                self.0
                    .entry(msg_id.0.clone())
                    .or_default()
                    .insert(*index as u16, buffer.clone());

                batch.push(buffer);
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
