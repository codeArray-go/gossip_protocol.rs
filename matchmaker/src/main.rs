use std::{
    collections::{HashSet, VecDeque},
    net::{SocketAddr, UdpSocket},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use p2p_lib::byte_converter::Serializer;

// Make 2 variable inside struct one to track whome to send list and them remove them and another for creating a list of address which we have to send don't remove element from it
struct NodesList {
    combined_list: HashSet<SocketAddr>,
    tracker: VecDeque<SocketAddr>,
}

fn main() {
    let socket = UdpSocket::bind("127.0.0.1:4200").expect("Failed to bind Udp Socket.");
    println!("Matchmaker server is Open on oprt: 4200");
    let socket_clone = socket.try_clone().expect("Failed to clone socket.");

    let node_list = Arc::new(Mutex::new(NodesList {
        combined_list: HashSet::new(),
        tracker: VecDeque::new(),
    }));

    let list_clone = node_list.clone();

    thread::spawn(move || {
        let mut buff = [0u8; 4];

        loop {
            if let Ok((_amt, src)) = socket_clone.recv_from(&mut buff) {
                let mut nodes_list = list_clone.lock().unwrap();
                if !nodes_list.tracker.contains(&src) {
                    nodes_list.tracker.push_back(src);
                    nodes_list.combined_list.insert(src);
                }
            }
        }
    });

    loop {
        let data = {
            let mut list = node_list.lock().unwrap();

            if list.combined_list.is_empty() {
                None
            } else {
                let target_add = list.tracker.pop_front().unwrap();
                let new_list = list.combined_list.clone();

                Some((target_add, new_list))
            }
        };

        let (target_add, new_list) = match data {
            Some(data) => data,

            None => {
                thread::sleep(Duration::from_millis(100));
                continue;
            }
        };

        let mut buffer = Vec::new();
        Serializer::serialize(&new_list, &mut buffer);

        match socket.send_to(&buffer, target_add) {
            Ok(_) => {
                println!("Sent node list to {}", target_add);
            }

            Err(e) => {
                println!("Failed to send to node: {}. Re-adding to queue.", e);
            }
        }

        let mut list = node_list.lock().unwrap();
        list.tracker.push_front(target_add);
    }
}
