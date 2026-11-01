use net2::UdpSocketExt;
use p2p_lib::{
    MsgType, U256,
    byte_converter::{Deserializer, Serializer},
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

use crate::utils::read_from_file;

pub mod utils;
struct NodeState {
    peer: Vec<SocketAddr>,
    msg_seen: HashSet<String>,
}

pub struct TypeBased {
    ack: BTreeMap<u16, Vec<SocketAddr>>,
    chat: BTreeMap<u16, Vec<u8>>,
}

#[derive(Default)]
pub struct ChunkVec(HashMap<U256, TypeBased>);

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
                    let crr_total_chunks: u16 = msg.total_chunks;
                    let curr_idx = msg.index as u16;

                    if !(curr_idx < crr_total_chunks) {
                        println!("Wrong chunk for wrong message id recieved.");
                        return;
                    }

                    // ACKNOWLEDGMENT
                    let msg_type = MsgType::Ack;
                    let mut id_buffer = vec![];
                    msg.id.serialize(&mut id_buffer);
                    let peers = vec![src];

                    let mut sended_chunk = ChunkSended::default();
                    let _to_sended_peer_id = sended_chunk.send_in_chunks(
                        msg_type,
                        &id_buffer,
                        None,
                        Some(crr_total_chunks),
                        peers,
                        &socket_clone,
                    );
                    // ;

                    // REPLY INSTANTLY IF IT'S RE OTHERWISE STORE VALUES
                }
            }
        }
    });

    let stdin = std::io::stdin();
    let mut input = String::new();

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

            let mut cache = sended_data.lock().unwrap();

            // TODO:- Handle error correctly as with error there will be ip of those whome failed to send chunks
            let msg_id = cache
                .send_in_chunks(msg_type, msg_byte, None, None, send_to_peers, &socket)
                .expect("Failed to send");

            let mut node = state.lock().unwrap();
            node.msg_seen.insert(format!("{:?}", msg_id));
        }
    }
}
