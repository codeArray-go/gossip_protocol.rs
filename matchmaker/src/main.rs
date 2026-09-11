use std::{
    collections::HashMap,
    net::{SocketAddr, UdpSocket},
    sync::{Arc, Mutex},
    thread,
};

use p2p_lib::byte_converter::Serializer;

struct NodeList(HashMap<SocketAddr, Vec<u8>>);

fn main() {
    let socket = UdpSocket::bind("0.0.0.1:4200").expect("Failed to bind Udp Socket.");
    println!("Matchmaker server is Open on oprt: 4200");
    let socket_clone = socket.try_clone().expect("Failed to clone socket.");

    let nodes_list = Arc::new(Mutex::new(NodeList(HashMap::new())));
    let nodes_list_clone = Arc::clone(&nodes_list);

    thread::spawn(move || {
        let mut buff = [0u8; 4];

        loop {
            if let Ok((amt, src)) = socket_clone.recv_from(&mut buff) {
                let mut node_list = nodes_list_clone.lock().unwrap();
                if node_list.0.contains_key(&src) {
                    println!("Node already exsting");
                }

                node_list.0.insert(src, buff[..amt].to_vec());

                // FILTEROUT SENDER IP
                let list_clone: HashMap<SocketAddr, Vec<u8>> = node_list
                    .0
                    .iter()
                    .filter(|&(k, _)| *k != src)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                let mut list_buffer = Vec::new();
                Serializer::serialize(&list_clone, &mut list_buffer);

                if list_clone.len() > 1 {
                    socket
                        .send_to(&list_buffer, src)
                        .expect("Error while sending");
                }
            }
        }
    });

    loop {
        println!("Hello");
    }
}
