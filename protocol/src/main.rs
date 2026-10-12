use net2::UdpSocketExt;
use p2p_lib::{
    MsgType, U256,
    byte_converter::Deserializer,
    utils::{Chunk, ChunkSended},
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    net::{SocketAddr, ToSocketAddrs, UdpSocket},
    path::Path,
    sync::{Arc, Mutex},
    thread, vec,
};

use crate::utils::{read_from_file, save_to_file};

pub mod utils;
struct NodeState {
    peer: Vec<SocketAddr>,
    msg_seen: HashSet<String>,
}

struct MessageState {
    index: u16,
    total_chunk: u16,
    recv_id: Option<U256>,
}

#[derive(Default)]
struct ChunkVec {
    recv_msg_vec: HashMap<U256, HashMap<MsgType, BTreeMap<u16, Vec<u8>>>>,
}

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
        peer: Vec::new(),
        msg_seen: HashSet::new(),
    }));

    // --- Local Discovery Setup (Temporary) ---
    // TODO: Remove this block once the Matchmaker server is hosted.
    let arg: Vec<String> = env::args().skip(1).collect();
    let port = &arg[0];

    let add = format!("0.0.0.0:{port}");
    println!("Running currenlty on: {:?}", &add);
    let socket = UdpSocket::bind(add).expect("Give a correct port.");
    // Increased buffer size at OS level till 2mb
    socket
        .set_send_buffer_size(1024 * 1024 * 2)
        .expect("Failed to set buffer size");
    let socket_clone = socket.try_clone().expect("Error while cloning.");

    // FILLED node.peers IF FILE ALREADY EXISTS THERE ELSE CONNECT TO MATCHMAKER

    // -----------------------------------------

    let chunk_vec = Arc::new(Mutex::new(ChunkVec {
        recv_msg_vec: HashMap::new(),
    }));
    let chunk_vec_clone = Arc::clone(&chunk_vec);

    let msg_state = Arc::new(Mutex::new(MessageState {
        index: 0,
        total_chunk: 0,
        recv_id: None,
    }));
    let msg_state_clone = Arc::clone(&msg_state);

    let state_clone = Arc::clone(&state);

    // LISTNER
    thread::spawn(move || {
        let mut buff = [0u8; 65_535];

        loop {
            if let Ok((amt, src)) = socket_clone.recv_from(&mut buff) {
                let mut actual_byte = &buff[..amt];

                let mut node = state_clone.lock().unwrap();
                if !node.peer.contains(&src) {
                    println!("New address connected: {:?}", src);
                    node.peer.push(src);
                }

                if let Ok(msg) = Chunk::deserialze(&mut actual_byte) {
                    let crr_total_chunks = msg.total_chunks as u16;
                    let curr_idx = msg.index as u16;

                    // Inserting value to MessageState and Cloning them for further use
                    let stored_total = {
                        let mut msg_state = msg_state_clone.lock().unwrap();

                        if msg_state.total_chunk == 0 || msg_state.index < crr_total_chunks {
                            msg_state.index = msg.index;
                            msg_state.total_chunk = crr_total_chunks;
                            msg_state.recv_id = Some(msg.id.clone());
                        }
                        msg_state.total_chunk
                    };

                    if !(curr_idx < stored_total) {
                        println!("Wrong chunk for wrong message id recieved.");
                        return;
                    }

                    // Storing chunk vec to ChunkVec
                    let mut chunk_vec = chunk_vec_clone.lock().unwrap();
                    chunk_vec
                        .recv_msg_vec
                        .entry(msg.id.clone())
                        .or_default()
                        .entry(msg.msg_type.clone())
                        .or_default()
                        .insert(curr_idx, msg.msg.to_vec());

                    // TODO: 1.Requesting for missing chunks
                    //  2. Complete code of acknowledgment

                    // Converting to real message
                    if let Some(chunks) = chunk_vec.recv_msg_vec.get(&msg.id)
                        && (chunks.len() as u16) == stored_total
                    {
                        let header = chunks.keys().next().unwrap();
                        let combined_byte: Vec<u8> = chunks
                            .get(header)
                            .unwrap()
                            .values()
                            .flatten()
                            .copied()
                            .collect();
                        match header {
                            MsgType::Chat => {
                                // TODO:- put a check for checking if any chunk is missing
                                let mut combo_byte_as_u8 = combined_byte.as_slice();
                                let message = String::deserialze(&mut combo_byte_as_u8);
                                println!(" [{:?}]:- message is: {:?}", src, message);
                                // TODO:- If any chunk got missed send request for it to back
                            }
                            MsgType::Ack => {
                                for byte in combined_byte {
                                    // Removing acknowledgmented chunks
                                    let id = U256::deserialze(&mut [byte].as_slice()).unwrap();
                                    chunk_vec.recv_msg_vec.remove_entry(&id);
                                }
                            }
                            MsgType::Re => {
                                // TODO:- send chunk again of index re-request came remove it from re-request
                                for byte in combined_byte {
                                    let id = U256::deserialze(&mut [byte].as_slice()).unwrap();
                                    if let Some(chunk) = chunk_vec.recv_msg_vec.get(&id) {
                                        if !chunk.contains_key(&MsgType::Re) {
                                            eprint!(
                                                "Failed to get re-send request from stored values"
                                            )
                                        }
                                        if let Some((_msg_type, tree)) = chunk.iter().next() {
                                            if let Some(index) = tree.keys().next() {
                                                if index == &msg.index {}
                                            }
                                        }
                                    }
                                }
                            }
                            MsgType::Peers => {
                                // SAVED PEER LIST TO FILE
                                let mut combo_byte_as_u8 = combined_byte.as_slice();
                                let peers: HashSet<SocketAddr> =
                                    HashSet::deserialze(&mut combo_byte_as_u8).unwrap();
                                let file_path = "addr.peer.bin";
                                save_to_file(file_path, &peers).expect("Failed to save to file");

                                // TODO:- If any chunk got missed send request for it to back
                            }
                        }
                    } else {
                        println!("Error in converting chunk to message");
                        return;
                    }
                }
            }
        }
    });

    let stdin = std::io::stdin();
    let mut input = String::new();
    let mut sender_cache = ChunkSended::default();

    // --- MESSAGE SENDER ---
    loop {
        input.clear();
        stdin.read_line(&mut input).unwrap();
        let text = input.trim().to_string();

        // -- PASSING FILE --
        // To pass file uncomment code and replace above 'text' variable name with 'path'
        // let text = fs::read_to_string(path).expect("Faild to read file");

        if !text.is_empty() {
            let peers: Vec<SocketAddr> = {
                let path = Path::new("./addr.peer.bin");
                let mut node = state.lock().unwrap();

                // TAKE PEER FROM LIST WHEN AVAILABLE ELSE FROM SERVER
                if node.peer.is_empty() {
                    read_from_file(path)
                        .ok()
                        .map(|addr| {
                            node.peer.extend(addr);
                            node.peer.clone()
                        })
                        .unwrap_or_else(|| {
                            "della-awakenings.tun.ply.gg:38576"
                                .to_socket_addrs()
                                .ok()
                                .and_then(|mut resolved_add| resolved_add.next())
                                .map(|peer_addr| vec![peer_addr])
                                .unwrap_or_default()
                        })
                } else {
                    node.peer.clone()
                }
            };

            let send_to_peers: Vec<SocketAddr> = peers.into_iter().take(10).collect();

            let msg_byte = text.as_bytes();
            let msg_type = p2p_lib::MsgType::Chat;

            // TODO:- Handle error correctly as with error there will be ip of those whome failed to send chunks
            let msg_id = sender_cache
                .send_in_chunks(msg_type, msg_byte, send_to_peers, &socket)
                .expect("Failed to send");

            let mut node = state.lock().unwrap();
            node.msg_seen.insert(format!("{:?}", msg_id));
        }
    }
}
