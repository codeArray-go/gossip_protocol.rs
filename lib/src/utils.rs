use crate::{
    Combined, MsgType, U256,
    byte_converter::{Deserializer, Serializer},
    hash::merkle_root,
};
use std::{
    collections::{BTreeMap, HashMap},
    io::ErrorKind,
    net::{SocketAddr, UdpSocket},
    thread,
};

// Chunk Strcture
pub struct Chunk<'a> {
    pub msg_type: MsgType,
    pub id: U256,
    pub msg: &'a [u8],
    pub index: u16,
    pub total_chunks: u16,
}

#[derive(Debug)]
pub struct SendedData {
    pub peers: Vec<SocketAddr>,
    pub chunks: BTreeMap<u16, Vec<u8>>,
    pub total: u16,
}

// Chunk Tracker (Cache)
#[derive(Default, Debug)]
pub struct ChunkSended(pub HashMap<U256, SendedData>);

// Message Serializer
impl<'a> Serializer for Chunk<'a> {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        self.msg_type.serialize(buffer);
        self.id.serialize(buffer);
        self.msg.serialize(buffer);
        self.index.serialize(buffer);
        self.total_chunks.serialize(buffer);
    }
}

// Message Deserializer
impl<'a> Deserializer<'a> for Chunk<'a> {
    fn deserialze(buffer: &mut &'a [u8]) -> std::prelude::v1::Result<Self, &'static str> {
        // Min header size: 32 (Hash) + 2 (index) + 2 (total_chunks) = 40 bytes
        const MIN_HEADER_SIZE: usize = 38;
        if buffer.len() < MIN_HEADER_SIZE {
            return Err("Buffer is too small for a valid message chunk");
        }

        // Msg_type
        let msg_type = MsgType::deserialze(buffer)?;

        // Message ID / Hash (32 bytes)
        let id = U256::deserialze(buffer)?;

        // Message Payload
        let msg = <&[u8] as Deserializer>::deserialze(buffer)?;

        // Chunk Index (2 bytes)
        let index = u16::deserialze(buffer)?;

        // Total Chunks count (2 bytes)
        let total_chunks = u16::deserialze(buffer)?;

        Ok(Chunk {
            msg_type,
            id,
            msg,
            index,
            total_chunks,
        })
    }
}

impl<'a> ChunkSended {
    // CREATE CHUNK AND THEN SEND TO ALL PEERS
    pub fn send_in_chunks(
        &mut self,
        msg_type: MsgType,
        msg: &[u8],
        id: Option<U256>,
        index: Option<u16>,
        peers: &[SocketAddr],
        socket: &UdpSocket,
    ) -> Result<U256, Vec<SocketAddr>> {
        const MAX_SAFE_PAYLOAD: usize = 1200;

        let msg_id = id.unwrap_or(merkle_root(&[Combined::Msg(msg), Combined::Kind(&msg_type)]).0);

        let all_packet: Vec<(usize, &[u8])> = msg.chunks(MAX_SAFE_PAYLOAD).enumerate().collect();

        let total_chunk = all_packet.len() as u16;

        let mut rm_ip: Vec<SocketAddr> = Vec::new();

        for chunk_batch in all_packet.chunks(25) {
            println!("Total Chunks in a batch: {:?}", chunk_batch.len());
            let mut batch = Vec::with_capacity(chunk_batch.len());

            for (idx, chunk_slice) in chunk_batch {
                let curr_index = index.unwrap_or(*idx as u16);
                let chunk = Chunk {
                    id: msg_id,
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

                // Saving Chunk to ChunkSended
                if msg_type == MsgType::Chat || msg_type == MsgType::Peers {
                    self.0
                        .entry(msg_id)
                        .and_modify(|msg_data| {
                            msg_data.chunks.insert(curr_index, buffer.clone());
                            msg_data.total = total_chunk;
                        })
                        .or_insert_with(|| {
                            let mut chunky = BTreeMap::new();
                            chunky.insert(curr_index, buffer.clone());

                            SendedData {
                                peers: Vec::new(),
                                chunks: chunky,
                                total: 0,
                            }
                        });
                }

                batch.push(buffer);
            }

            // Seding Message to 5 active peers only
            for peer_list in peers.chunks(5) {
                let mut active_peers = peer_list.to_vec();

                for packet in &batch {
                    active_peers.retain(|peer| match socket.send_to(packet, peer) {
                        Ok(_) => {
                            return true;
                        }
                        Err(e) => {
                            // CHECKING IF OS BUFFER FILLED COMPLETELY
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

                // Saving peers to ChunkSended
                if msg_type == MsgType::Chat {
                    self.0.entry(msg_id).and_modify(|data| {
                        data.peers = active_peers; // Correct this as if there will be re-request for only one peer it'll remove other peers 
                    });
                }
            }
            // ;
        }

        if !rm_ip.is_empty() {
            return Err(rm_ip);
        }
        Ok(msg_id)
    }
}
