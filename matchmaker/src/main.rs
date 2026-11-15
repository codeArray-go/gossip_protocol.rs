use p2p_lib::{
    MsgType,
    byte_converter::{Deserializer, Serializer},
    utils::ChunkSended,
};
use std::{
    collections::{BTreeMap, HashMap},
    net::{SocketAddr, UdpSocket},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

struct NodesList {
    combined_list: Vec<SocketAddr>,
    tracker: Vec<SocketAddr>,
}

fn main() {
    let add = format!("127.0.0.1:4200");

    let socket = UdpSocket::bind(&add).expect("Failed to bind Udp Socket.");
    println!("Matchmaker server is Open on: {add}");
    let socket_clone = socket.try_clone().expect("Failed to clone socket.");

    let node_list = Arc::new(Mutex::new(NodesList {
        combined_list: Vec::new(),
        tracker: Vec::new(),
    }));

    let list_clone = Arc::clone(&node_list);

    let mut sender_cache = ChunkSended::default();

    thread::spawn(move || {
        let mut buff = [0u8; 4];

        // --- LISTNER ---
        loop {
            if let Ok((amt, src)) = socket_clone.recv_from(&mut buff) {
                let mut actual_byte = &buff[..amt];

                let msg_type = MsgType::deserialze(&mut actual_byte).unwrap();

                println!("{:?}", msg_type);

                if msg_type != MsgType::Ack {
                    let mut nodes_list = list_clone.lock().unwrap();
                    if !nodes_list.tracker.contains(&src) {
                        nodes_list.tracker.push(src);
                        nodes_list.combined_list.push(src);
                    }
                }

                // Add logic if msg type is ack then remove msg from tracker
            }
        }
    });

    loop {
        let data = 'node_list: {
            let list = node_list.lock().unwrap();

            if list.tracker.is_empty() {
                break 'node_list None;
            }

            if list.combined_list.is_empty() {
                None
            } else {
                let send_to_peers = list.tracker.clone();
                let new_list = list.combined_list.clone();

                Some((send_to_peers, new_list))
            }
        };

        let (send_to_peers, new_list) = match data {
            Some(data) => data,

            None => {
                thread::sleep(Duration::from_millis(100));
                continue;
            }
        };

        let mut buffer: Vec<u8> = vec![];
        new_list.serialize(&mut buffer);

        println!("{:?}", send_to_peers);

        match sender_cache.send_in_chunks(
            MsgType::Peers,
            &buffer,
            None,
            None,
            &send_to_peers,
            &socket,
        ) {
            Ok(_msg_id) => {}
            Err(lst) => {
                // Adding ip back to target if failed to send packet
                println!("Adding peer back to queue");
                let mut list = node_list.lock().unwrap();

                for peer in lst {
                    list.tracker.push(peer);
                }
            }
        }
    }
}
