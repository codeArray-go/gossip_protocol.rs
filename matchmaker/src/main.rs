use p2p_lib::{byte_converter::Serializer, hash::Hash, utils::ChunkSended};
use std::{
    collections::HashSet,
    env,
    net::{SocketAddr, UdpSocket},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

struct NodesList {
    combined_list: HashSet<SocketAddr>,
    tracker: Vec<SocketAddr>,
}

fn main() {
    let port = env::var("PORT").unwrap_or_else(|_| "4200".to_string());
    let add = format!("0.0.0.0:{port}");

    let socket = UdpSocket::bind(&add).expect("Failed to bind Udp Socket.");
    println!("Matchmaker server is Open on: {add}");
    let socket_clone = socket.try_clone().expect("Failed to clone socket.");

    let node_list = Arc::new(Mutex::new(NodesList {
        combined_list: HashSet::new(),
        tracker: Vec::new(),
    }));

    let list_clone = node_list.clone();

    let mut sender_cache = ChunkSended::default();

    thread::spawn(move || {
        let mut buff = [0u8; 4];

        loop {
            if let Ok((_amt, src)) = socket_clone.recv_from(&mut buff) {
                let mut nodes_list = list_clone.lock().unwrap();
                if !nodes_list.tracker.contains(&src) {
                    nodes_list.tracker.push(src);
                    nodes_list.combined_list.insert(src);
                }
            }
        }
    });

    loop {
        let data = {
            let list = node_list.lock().unwrap();

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

        let mut buffer = Vec::new();
        Serializer::serialize(&new_list, &mut buffer);

        let msg_id = Hash::of(&new_list);

        let peer_quantity = send_to_peers.len() as usize;

        match sender_cache.send_in_chunks(&buffer, msg_id, send_to_peers, &socket) {
            Ok(()) => {
                // Removed those ip whome list is sended successfully
                let mut list = node_list.lock().unwrap();
                let curr_list_len = list.tracker.len() as usize;
                list.tracker
                    .drain(curr_list_len.saturating_sub(peer_quantity)..);
            }
            Err(lst) => {
                // Adding ip back to target if faild to send packet
                println!("Adding peer back to queue");
                let mut list = node_list.lock().unwrap();

                for peer in lst {
                    list.tracker.push(peer);
                }
            }
        }
    }
}
