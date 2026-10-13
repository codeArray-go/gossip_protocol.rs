use crate::{
    Combined, MsgType, U256,
    byte_converter::{Deserializer, Serializer},
    hash::{Hash, merkle_root},
};
use std::{
    collections::{BTreeMap, HashMap},
    io::ErrorKind,
    net::{SocketAddr, UdpSocket},
    thread,
};

pub struct Chunk<'a> {
    pub id: U256,
    pub msg_type: MsgType,
    pub msg: &'a [u8],
    pub index: u16,
    pub total_chunks: u16,
}

// CHUNK TRACKER
#[derive(Default)]
pub struct ChunkSended(HashMap<U256, BTreeMap<u16, Vec<u8>>>);

impl<'a> Serializer for Chunk<'a> {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        self.id.serialize(buffer);
        self.msg_type.serialize(buffer);
        self.msg.serialize(buffer);
        self.index.serialize(buffer);
        self.total_chunks.serialize(buffer);
    }
}

impl<'a> Deserializer<'a> for Chunk<'a> {
    fn deserialze(buffer: &mut &'a [u8]) -> std::prelude::v1::Result<Self, &'static str> {
        // Min header size: 32 (Hash) + 4 (msg_len) + 2 (index) + 2 (total_chunks) = 40 bytes
        const MIN_HEADER_SIZE: usize = 40;
        if buffer.len() < MIN_HEADER_SIZE {
            return Err("Buffer is too small for a valid message chunk");
        }

        // Message ID / Hash (32 bytes)
        let id = U256::deserialze(buffer)?;

        // Msg_type
        let msg_type = MsgType::deserialze(buffer)?;

        // Message Payload
        let msg = <&[u8] as Deserializer>::deserialze(buffer)?;

        // Chunk Index (2 bytes)
        let index = u16::deserialze(buffer)?;

        // Total Chunks count (2 bytes)
        let total_chunks = u16::deserialze(buffer)?;

        Ok(Chunk {
            id,
            msg_type,
            msg,
            index,
            total_chunks,
        })
    }
}

// CREATE CHUNK AND THEN SEND TO ALL PEERS
impl<'a> ChunkSended {
    pub fn send_in_chunks(
        &mut self,
        msg_type: MsgType,
        msg: &[u8],
        index: Option<u16>,
        peers: Vec<SocketAddr>,
        socket: &UdpSocket,
    ) -> Result<Hash, Vec<SocketAddr>> {
        const MAX_SAFE_PAYLOAD: usize = 1400;

        let msg_id = merkle_root(&[Combined::Msg(msg), Combined::Kind(&msg_type)]);

        let all_packet: Vec<(usize, &[u8])> = msg.chunks(MAX_SAFE_PAYLOAD).enumerate().collect();

        let total_chunk = all_packet.len() as u16;

        let mut rm_ip: Vec<SocketAddr> = Vec::new();

        for chunk_batch in all_packet.chunks(25) {
            let mut batch = Vec::with_capacity(chunk_batch.len());

            for (idx, chunk_slice) in chunk_batch {
                let curr_index = index.unwrap_or(*idx as u16);
                let chunk = Chunk {
                    id: msg_id.0,
                    msg_type,
                    msg: chunk_slice,
                    index: curr_index,
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
                    .insert(curr_index, buffer.clone());

                batch.push(buffer);
            }

            for peer_list in peers.chunks(5) {
                let mut active_peers = peer_list.to_vec();

                for packet in &batch {
                    active_peers.retain(|peer| match socket.send_to(packet, peer) {
                        Ok(_) => {
                            return true;
                        }
                        Err(e) => {
                            // Checking if OS buffer filled completely
                            if e.kind() == ErrorKind::WouldBlock
                                || e.kind() == ErrorKind::Interrupted
                            {
                                thread::yield_now();
                                true
                            } else {
                                eprintln!("Error: {e}");
                                rm_ip.push(*peer);
                                false
                            }
                        }
                    });

                    if active_peers.is_empty() {
                        break;
                    }
                }
            }
        }

        if !rm_ip.is_empty() {
            return Err(rm_ip);
        }

        Ok(msg_id)
    }
}
