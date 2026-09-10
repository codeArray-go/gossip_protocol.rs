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
                let actual_bytes = &buff[..amt];
                let mut node_list = nodes_list_clone.lock().unwrap();
                if node_list.0.contains_key(&src) {
                    println!("Node with address: {src} is already available");
                    return;
                }

                node_list.0.insert(src.clone(), actual_bytes.to_vec());
                let copy_list = node_list.0.clone();

                let mut buff: Vec<u8> = Vec::new();
                let list_byte = Serializer::serialize(&copy_list, &mut buff);

                if copy_list.len() > 1 {
                    // TODO: replace actual_bytes with real data to send
                    socket_clone
                        .send_to(actual_bytes, src)
                        .expect("Failed to send address of: ");
                }
            }
        }
    });

    loop {
        println!("Hello");
    }
}
