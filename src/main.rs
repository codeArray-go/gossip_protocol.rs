use crate::utils::{Chunk, Hash, U256, send_in_chunks};
use std::{
    collections::{HashMap, HashSet},
    env,
    // fs::{self, File},
    // io::Write,
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
    total_chunk: u16,
    recv_id: Option<U256>,
}

#[derive(Default)]
struct ChunkVec {
    recv_msg_vec: HashMap<U256, Vec<Vec<u8>>>,
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
    // TODO: Remove this block once the Relay server is implemented.
    let arg: Vec<String> = env::args().skip(1).collect();

    let socket = UdpSocket::bind(&arg[0]).expect("Give a correct port.");
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
                let actual_byte = &buff[..amt];

                let mut node = state_clone.lock().unwrap();
                if !node.peer.contains(&src) {
                    println!("New address connected: {:?}", src);
                    node.peer.push(src);
                }

                if let Ok(msg) = Chunk::from_byte(actual_byte) {
                    let total_chunks = msg.total_chunks as u16;
                    let index = msg.index as u16;

                    let mut msg_state = msg_state_clone.lock().unwrap();
                    if msg_state.total_chunk == 0 {
                        msg_state.total_chunk = total_chunks;
                        msg_state.recv_id = Some(msg.id.clone());
                    }

                    let mut chunk_vec = chunk_vec_clone.lock().unwrap();
                    if msg_state.recv_id.unwrap() == msg.id {
                        if index < msg_state.total_chunk && msg.id == msg_state.recv_id.unwrap() {
                            chunk_vec
                                .recv_msg_vec
                                .entry(msg.id.clone())
                                .or_default()
                                .push(msg.msg.to_vec());
                        }
                    }

                    if index == (msg_state.total_chunk - 1) {
                        if let Some(chunks) = chunk_vec.recv_msg_vec.get(&msg.id) {
                            // CHECKING IF CHUNK IS MISSING
                            if (chunks.len() as u16) < msg_state.total_chunk {
                                println!("packets are missing");
                                // TODO: add logic if some chunk is missing then ask for specific index chunk
                            }
                            let combined_byte: Vec<u8> = chunks.concat();

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
                        }
                    }
                }
            }
        }
    });

    println!("Type your message and press enter to send. ");

    let stdin = std::io::stdin();
    let mut input = String::new();
    let clone_state = Arc::clone(&state);

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
                let mut node = clone_state.lock().unwrap();
                node.msg_seen.insert(format!("{:?}", msg_id));
                node.peer.clone()
            };

            send_in_chunks(&text, msg_id, peers, &socket);
        }
    }
}
