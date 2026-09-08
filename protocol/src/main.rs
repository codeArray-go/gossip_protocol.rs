use crate::utils::Chunk;
use net2::UdpSocketExt;
use p2p_lib::{U256, byte_converter::Deserializer, hash::Hash};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    net::{SocketAddr, UdpSocket},
    sync::{Arc, Mutex},
    thread,
};

mod utils;

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
    recv_msg_vec: HashMap<U256, BTreeMap<u16, Vec<u8>>>,
}

#[derive(Default)]
pub struct ChunkSended(HashMap<U256, BTreeMap<u16, Vec<u8>>>);

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
    // TODO: Remove this block once the Relay server is implemented.
    let arg: Vec<String> = env::args().skip(1).collect();

    let socket = UdpSocket::bind(&arg[0]).expect("Give a correct port.");
    // Increased buffer size at OS level till 2mb
    socket
        .set_send_buffer_size(1024 * 1024 * 2)
        .expect("Failed to set buffer size");
    let socket_clone = socket.try_clone().expect("Error while cloning.");

    if arg.len() > 1 {
        let mut node = state.lock().unwrap();
        let addrs = &arg[1];
        if let Ok(peers) = addrs.parse::<SocketAddr>() {
            node.peer.push(peers);
            println!("Peer connected: {peers}");
        } else {
            println!("Wrong address provided.");
        }
    }
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
                    let (stored_total, _stored_idx, stored_recv_id) = {
                        let mut msg_state = msg_state_clone.lock().unwrap();

                        if msg_state.total_chunk == 0 || msg_state.total_chunk < crr_total_chunks {
                            msg_state.index = msg.index;
                            msg_state.total_chunk = crr_total_chunks;
                            msg_state.recv_id = Some(msg.id.clone());
                        }
                        (
                            msg_state.total_chunk,
                            msg_state.index,
                            msg_state.recv_id.clone(),
                        )
                    };

                    if !(curr_idx < stored_total) && !(msg.id == stored_recv_id.unwrap()) {
                        println!("Wrong chunk for wrong message id recieved.");
                        return;
                    }

                    // Storing chunk vec to ChunkVec
                    let mut chunk_vec = chunk_vec_clone.lock().unwrap();
                    chunk_vec
                        .recv_msg_vec
                        .entry(msg.id.clone())
                        .or_default()
                        .insert(curr_idx, msg.msg.to_vec());

                    // TODO: 1.Requesting for missing chunks
                    //  2. Complete code of acknowledgment

                    // Converting to real message
                    if let Some(chunks) = chunk_vec.recv_msg_vec.get(&msg.id)
                        && (chunks.len() as u16) == stored_total
                    {
                        let combined_byte: Vec<u8> = chunks.values().flatten().copied().collect();
                        match String::from_utf8(combined_byte) {
                            Ok(message) => {
                                node.msg_seen.insert(format!("{:?}", msg.id));
                                println!("[{:?}]:- msg: {:?}", src, message);
                            }
                            Err(e) => {
                                println!("Error: Bytes are not valid UTF-8 text: {}", e);
                                chunk_vec.recv_msg_vec.remove(&msg.id);
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

    println!("Type your message and press enter to send. ");

    let stdin = std::io::stdin();
    let mut input = String::new();
    let mut sender_cache = ChunkSended::default();

    // MESSAGE SENDER
    loop {
        input.clear();
        stdin.read_line(&mut input).unwrap();
        let text = input.trim().to_string();

        // -- PASSING FILE --
        // -- TODO: To pass file uncomment code and replace above 'text' variable name with 'path'
        // let text = fs::read_to_string(path).expect("Faild to read file");

        if !text.is_empty() {
            let msg_id = Hash::of(&text);

            let peers: Vec<SocketAddr> = {
                let mut node = state.lock().unwrap();
                node.msg_seen.insert(format!("{:?}", msg_id));
                node.peer.clone()
            };

            let send_to_peers: Vec<SocketAddr> = peers.into_iter().take(10).collect();

            sender_cache.send_in_chunks(&text, msg_id, send_to_peers, &socket);

            // TODO:- After aknowledgment from receiver remove chunks from ChunkSended
        }
    }
}
