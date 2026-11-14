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

use crate::utils::{read_from_file, save_to_file};
pub mod utils;

struct NodeState {
    peer: Arc<Vec<SocketAddr>>,
    msg_seen: HashSet<String>,
}

enum TypeBased {
    Peers((usize, Vec<u8>)),
    Message {
        ack: BTreeMap<u16, Vec<SocketAddr>>,
        chat: BTreeMap<u16, Vec<u8>>,
        removed_acks: u16,
    },
}

#[derive(Default)]
struct ChunkVec(HashMap<U256, TypeBased>);

struct CompletelyReceivedMsg(VecDeque<U256>);

fn main() {
    // -------------------------------------------------------------------------
    // TEST UTILITY: 10MB Payload Generator
    // Uncomment the commented code below to generate a dummy text file for chunk testing.
    // -------------------------------------------------------------------------
    // let mut file = File::create("text.txt").expect("Failed to create file.");
    //
    // for i in 1..=200_00 {
    //     let line = format!(
    //         "Message block {} - Testing UDP chunking logic in Rust. Repeating to reach 10MB payload size...\n",
    //         i
    //     );
    //     file.write_all(line.as_bytes()).unwrap();
    // }
    // println!("File gerated.");

    let state = Arc::new(Mutex::new(NodeState {
        peer: Arc::new(Vec::new()),
        msg_seen: HashSet::new(),
    }));

    // --- OWN PORT ENTRY (remove this once setup start creating it's own port) ---
    let arg: Vec<String> = env::args().skip(1).collect();
    let port = &arg[0];

    let add = format!("0.0.0.0:{port}");
    println!("Running currenlty on: {:?}", &add);
    let socket = UdpSocket::bind(add).expect("Give a correct port.");

    // INCREASE MEMORY BUFFER SIZE TO 2MB
    let new_size = 1024 * 1024 * 2;
    socket
        .set_send_buffer_size(new_size)
        .expect("Failed to set buffer size at sender end");

    socket
        .set_recv_buffer_size(new_size)
        .expect("Failed to set buffer size at receving end");

    let socket_clone = socket.try_clone().expect("Error while cloning.");

    // FILLED node.peers IF FILE ALREADY EXISTS THERE ELSE CONNECT TO MATCHMAKER

    // -----------------------------------------

    let chunk_vec = Arc::new(Mutex::new(ChunkVec(HashMap::new())));
    let chunk_vec_clone = Arc::clone(&chunk_vec);

    let state_clone = Arc::clone(&state);

    let sended_data = Arc::new(Mutex::new(ChunkSended(HashMap::new())));
    let sended_data_clone = Arc::clone(&sended_data);

    let completed_msg = Arc::new(Mutex::new(CompletelyReceivedMsg(VecDeque::new())));
    let completed_msg_clone = Arc::clone(&completed_msg);

    // LISTNER
    thread::spawn(move || {
        let mut buff = [0u8; 65_535];

        loop {
            if let Ok((amt, src)) = socket_clone.recv_from(&mut buff) {
                let mut actual_byte = &buff[..amt];

                {
                    let mut node = state_clone.lock().unwrap();
                    if !node.peer.contains(&src) {
                        println!("New address connected: {:?}", src);
                        let mutable_vec = Arc::make_mut(&mut node.peer);
                        mutable_vec.push(src);
                    }
                }

                if let Ok(msg) = Chunk::deserialze(&mut actual_byte) {
                    let crr_total_chunks: u16 = msg.total_chunks;
                    let curr_idx = msg.index as u16;

                    if !(curr_idx < crr_total_chunks) {
                        println!("Wrong chunk for wrong message id recieved.");
                        return;
                    }

                    // ACKNOWLEDGMENT
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
                    // ;

                    {
                        let completed_msg = completed_msg_clone.lock().unwrap();
                        if completed_msg.0.contains(&msg.id) {
                            return;
                        }
                    }

                    // REPLY INSTANTLY IF IT'S RE OTHERWISE STORE VALUES
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
                                    let sended = sended_data_clone.lock().unwrap();
                                    let target_peer_len =
                                        sended.0.get(&msg.id).unwrap().peers.len();

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
                            let mut sended_data = sended_data_clone.lock().unwrap();
                            let chunk = sended_data
                                .0
                                .get(&msg.id)
                                .unwrap()
                                .chunks
                                .get(&msg.index)
                                .unwrap()
                                .clone();

                            let _id = sended_data.send_in_chunks(
                                MsgType::Chat,
                                &chunk,
                                Some(msg.id),
                                Some(msg.index),
                                &[src],
                                &socket_clone,
                            );
                        }
                        MsgType::Peers => {
                            // SAVED PEER LIST TO FILE
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

                            if let TypeBased::Peers((_, val)) = all_bytes {
                                let all_peers: HashSet<SocketAddr> =
                                    HashSet::deserialze(&mut val.as_slice()).unwrap();

                                let file_path = "addr.peer.bin";

                                let mut exiting_peers: Vec<SocketAddr> =
                                    match read_from_file(file_path) {
                                        Ok(peers) => peers,
                                        Err(_) => Vec::new(),
                                    };

                                exiting_peers.extend(all_peers);

                                save_to_file(file_path, &exiting_peers)
                                    .expect("Failed to save to file");
                            }
                        }
                    }
                }
            }
        }
    });

    let stdin = std::io::stdin();
    let mut input = String::new();
    println!("Type text and press enter to send");

    // --- MESSAGE SENDER ---
    loop {
        input.clear();
        stdin.read_line(&mut input).unwrap();
        let text = input.trim().to_string();

        // -- PASSING FILE --
        // To pass file uncomment code and replace above 'text' variable name with 'path'
        // let text = fs::read_to_string(path).expect("Faild to read file");

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

                if fetched_peers.is_empty() {
                    if let Ok(mut resolved) = "della-awakenings.tun.ply.gg:38576".to_socket_addrs()
                    {
                        if let Some(addr) = resolved.next() {
                            fetched_peers.push(addr)
                        }
                    }
                }

                let mut node = state.lock().unwrap();
                if node.peer.is_empty() {
                    node.peer = Arc::new(fetched_peers);
                    node.peer.clone()
                } else {
                    node.peer.clone()
                }
            };

            let send_to_peers: Vec<SocketAddr> = peers.into_iter().take(10).collect();

            let msg_byte = text.as_bytes();
            let msg_type = p2p_lib::MsgType::Chat;

            let mut cache = sended_data.lock().unwrap();

            // TODO:- Handle error correctly as with error there will be ip of those whome failed to send chunks
            let msg_id = cache
                .send_in_chunks(msg_type, msg_byte, None, None, &send_to_peers, &socket)
                .expect("Failed to send");

            let mut node = state.lock().unwrap();
            node.msg_seen.insert(format!("{:?}", msg_id));
        }
    }
}
