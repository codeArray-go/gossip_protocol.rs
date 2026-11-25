use crate::utils::{read_from_file, save_to_file};
use dashmap::DashMap;
use net2::UdpSocketExt;
use p2p_lib::{
    MsgType, U256,
    byte_converter::Deserializer,
    utils::{Chunk, ChunkSended},
};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    env,
    net::{SocketAddr, ToSocketAddrs, UdpSocket},
    path::Path,
    sync::{Arc, Mutex},
    thread, vec,
};
pub mod utils;

// Storage for Address
struct NodeState {
    peer: Arc<Vec<SocketAddr>>,
    msg_seen: HashSet<String>,
}

// Type based distribution
pub enum TypeBased {
    Peers((usize, Vec<u8>)),
    Message {
        ack: BTreeMap<u16, Vec<SocketAddr>>,
        chat: BTreeMap<u16, Vec<u8>>,
        removed_acks: u16,
    },
}

// Storage for Chunks recieved
#[derive(Default)]
struct ChunkVec(HashMap<U256, TypeBased>);

// Storage for Messages which are Deserialized successfully
struct CompletelyReceivedMsg(VecDeque<U256>);

fn main() {
    let state = Arc::new(Mutex::new(NodeState {
        peer: Arc::new(Vec::new()),
        msg_seen: HashSet::new(),
    }));

    // My Listener or Sender PORT Entry
    let arg: Vec<String> = env::args().skip(1).collect();
    let port = &arg[0];

    let add = format!("0.0.0.0:{port}");
    println!("Running currenlty on: {:?}", &add);
    let socket = UdpSocket::bind(add).expect("Give a correct port.");
    // ;

    // Increases memory buffer size to 2MB
    let new_size = 1024 * 1024 * 2;
    socket
        .set_send_buffer_size(new_size)
        .expect("Failed to set buffer size at sender end");

    socket
        .set_recv_buffer_size(new_size)
        .expect("Failed to set buffer size at receving end");

    let socket_clone = socket.try_clone().expect("Error while cloning.");
    // ;

    let chunk_vec = Arc::new(Mutex::new(ChunkVec(HashMap::new())));
    let chunk_vec_clone = Arc::clone(&chunk_vec);

    let state_clone = Arc::clone(&state);

    let mut sended_data = Arc::new(ChunkSended(DashMap::new()));
    let mut sended_data_clone = Arc::clone(&sended_data);

    let completed_msg = Arc::new(Mutex::new(CompletelyReceivedMsg(VecDeque::new())));
    let completed_msg_clone = Arc::clone(&completed_msg);

    // -- LISTNER --
    thread::spawn(move || {
        let mut buff = [0u8; 65_535];

        loop {
            if let Ok((amt, src)) = socket_clone.recv_from(&mut buff) {
                let mut actual_byte = &buff[..amt];

                // Saving address if it's not matchmaker
                {
                    if src
                        != "della-awakenings.tun.ply.gg:38576"
                            .to_socket_addrs()
                            .unwrap()
                            .next()
                            .unwrap()
                    {
                        let mut node = state_clone.lock().unwrap();
                        if !node.peer.contains(&src) {
                            println!("New address connected: {:?}", src);
                            let mutable_vec = Arc::make_mut(&mut node.peer);
                            mutable_vec.push(src);
                        }
                    }
                }
                // ;

                if let Ok(msg) = Chunk::deserialze(&mut actual_byte) {
                    // Check for extra chunks if recieved even message is completed
                    {
                        let completed_msg = completed_msg_clone.lock().unwrap();
                        if completed_msg.0.contains(&msg.id) {
                            return;
                        }
                    }
                    // ;

                    let crr_total_chunks: u16 = msg.total_chunks;
                    let curr_idx = msg.index as u16;

                    if !(curr_idx < crr_total_chunks) {
                        println!("Wrong chunk for wrong message id recieved.");
                        return;
                    }

                    // Acknowledgment
                    if msg.msg_type != MsgType::Ack {
                        let msg_type = MsgType::Ack;
                        let ack_text = "Done";
                        let ack_text_byte = ack_text.as_bytes();

                        let mut sended_chunk = ChunkSended::default();
                        let _to_sended_peer_id = sended_chunk.send_in_chunks(
                            msg_type,
                            &ack_text_byte,
                            Some(msg.id),
                            Some(msg.index),
                            &[src],
                            &socket_clone,
                        );
                    };
                    // ;

                    // Deserializing Message based on it's Header Type
                    match msg.msg_type {
                        MsgType::Chat => {
                            let mut chunk_vec = chunk_vec_clone.lock().unwrap();
                            let chunks = chunk_vec
                                .0
                                .entry(msg.id)
                                .and_modify(|data| {
                                    if let TypeBased::Message { chat, .. } = data {
                                        chat.insert(msg.index, msg.msg.to_vec());
                                    }
                                })
                                .or_insert_with(|| {
                                    let mut new = BTreeMap::new();
                                    new.insert(msg.index, msg.msg.to_vec());

                                    TypeBased::Message {
                                        ack: BTreeMap::new(),
                                        chat: new,
                                        removed_acks: 0,
                                    }
                                });

                            // TODO: add logic for sending rerequest if chunk is missing
                            let is_completed = if let TypeBased::Message { chat, .. } = chunks {
                                chat.len() == msg.total_chunks as usize
                            } else {
                                return;
                            };

                            if is_completed {
                                let extracted_chunks = chunk_vec.0.remove(&msg.id).unwrap();
                                if let TypeBased::Message { chat, .. } = extracted_chunks {
                                    let all_chunk_vec: Vec<u8> =
                                        chat.into_values().flatten().collect();

                                    let text = String::deserialze(&mut all_chunk_vec.as_slice());

                                    let mut completed_msg = completed_msg_clone.lock().unwrap();
                                    if completed_msg.0.len() == 1000 {
                                        completed_msg.0.pop_front();
                                    }

                                    completed_msg.0.push_back(msg.id);

                                    println!("{:?}: {:?}", src, text);
                                }
                            }
                        }
                        MsgType::Ack => {
                            let mut chunk_vec = chunk_vec_clone.lock().unwrap();
                            let chunk = chunk_vec
                                .0
                                .entry(msg.id)
                                .and_modify(|data| {
                                    if let TypeBased::Message { ack, .. } = data {
                                        ack.entry(msg.index)
                                            .and_modify(|val| {
                                                val.push(src);
                                            })
                                            .or_insert_with(|| vec![src]);
                                    }
                                })
                                .or_insert_with(|| {
                                    let mut new = BTreeMap::new();
                                    new.insert(msg.index, vec![src]);

                                    TypeBased::Message {
                                        ack: new,
                                        chat: BTreeMap::new(),
                                        removed_acks: 0,
                                    }
                                });

                            if let TypeBased::Message {
                                ack, removed_acks, ..
                            } = chunk
                            {
                                if ack.len() == (msg.total_chunks - *removed_acks) as usize {
                                    let target_peer_len =
                                        sended_data_clone.0.get(&msg.id).unwrap().peers.len();

                                    ack.retain(|_key, val| {
                                        if val.len() == target_peer_len {
                                            *removed_acks += 1;
                                            false
                                        } else {
                                            true
                                        }
                                    });
                                } else {
                                    return;
                                }
                            }
                        }
                        MsgType::Re => {
                            let chunk = sended_data_clone
                                .0
                                .get(&msg.id)
                                .unwrap()
                                .chunks
                                .get(&msg.index)
                                .unwrap()
                                .clone();

                            let _id = Arc::get_mut(&mut sended_data_clone)
                                .unwrap()
                                .send_in_chunks(
                                    MsgType::Chat,
                                    &chunk,
                                    Some(msg.id),
                                    Some(msg.index),
                                    &[src],
                                    &socket_clone,
                                );
                        }
                        MsgType::Peers => {
                            let all_bytes = {
                                let mut chunk_vec = chunk_vec_clone.lock().unwrap();

                                let entry = chunk_vec
                                    .0
                                    .entry(msg.id)
                                    .or_insert_with(|| TypeBased::Peers((0, Vec::new())));

                                let all_peers = if let TypeBased::Peers((total_count, val)) = entry
                                {
                                    val.extend_from_slice(msg.msg);
                                    *total_count += 1;

                                    *total_count == msg.total_chunks as usize
                                } else {
                                    false
                                };

                                if all_peers {
                                    chunk_vec.0.remove(&msg.id).unwrap()
                                } else {
                                    return;
                                }
                            };

                            // Saving Peer-List to file
                            if let TypeBased::Peers((_, val)) = all_bytes {
                                let all_peers: Vec<SocketAddr> =
                                    Vec::deserialze(&mut val.as_slice()).unwrap();

                                let file_path = "addr.peer.bin";

                                let mut exiting_peers: Vec<SocketAddr> =
                                    match read_from_file(file_path) {
                                        Ok(peers) => peers,
                                        Err(_) => Vec::new(),
                                    };

                                exiting_peers.extend(all_peers);

                                save_to_file(file_path, &exiting_peers)
                                    .expect("Failed to save to file");

                                let mut node = state_clone.lock().unwrap();
                                let mutable = Arc::make_mut(&mut node.peer);

                                mutable.extend(exiting_peers);
                            }
                            // ;
                        }
                    }
                }
            }
        }
    });
    // ;

    // -- SENDER --
    let stdin = std::io::stdin();
    let mut input = String::new();
    println!("Type text and press enter to send");

    loop {
        stdin.read_line(&mut input).unwrap();
        let text = input.trim().to_string();

        if !text.is_empty() {
            let peers: Arc<Vec<SocketAddr>> = 'get_peers: {
                {
                    let node = state.lock().unwrap();
                    if !node.peer.is_empty() {
                        break 'get_peers node.peer.clone();
                    }
                }

                let path = Path::new("./addr.peer.bin");
                let mut fetched_peers: Vec<SocketAddr> = read_from_file(path).unwrap_or_default();

                // Checking if file with Peer-List already exists if not then using matchmaker ip as peer
                if fetched_peers.is_empty() {
                    if let Ok(mut resolved) = "della-awakenings.tun.ply.gg:38576".to_socket_addrs()
                    {
                        if let Some(addr) = resolved.next() {
                            fetched_peers.push(addr)
                        }
                    }
                }
                // ;

                // Checking if Peer-List Exists else add List from File recieved from Matchmaker
                let mut node = state.lock().unwrap();
                if node.peer.is_empty() {
                    node.peer = Arc::new(fetched_peers);
                    node.peer.clone()
                } else {
                    node.peer.clone()
                }
                // ;
            };

            let send_to_peers = peers.clone();

            let msg_byte = text.as_bytes();
            let msg_type = p2p_lib::MsgType::Chat;

            // TODO:- Handle error correctly as with error there will be ip of those whome failed to send chunks
            let cache = Arc::get_mut(&mut sended_data).unwrap();
            let msg_id = cache
                .send_in_chunks(msg_type, msg_byte, None, None, &send_to_peers, &socket)
                .expect("Failed to send");

            let mut node = state.lock().unwrap();
            node.msg_seen.insert(format!("{:?}", msg_id));

            Arc::make_mut(&mut node.peer).retain(|add| {
                *add != "della-awakenings.tun.ply.gg:38576"
                    .to_socket_addrs()
                    .unwrap()
                    .next()
                    .unwrap()
            });
        }

        input.clear();
    }
    // ;
}
