use dashmap::DashMap;
use p2p_lib::{
    MsgType, U256,
    byte_converter::{Deserializer, Serializer},
    utils::{Chunk, ChunkSended},
};
use std::{
    collections::BTreeMap,
    net::{SocketAddr, UdpSocket},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

struct NodesList {
    combined_list: Vec<SocketAddr>,
    tracker: Vec<SocketAddr>,
}

struct DataType {
    ack: BTreeMap<u16, Vec<SocketAddr>>,
    removed_ack: u16,
}

struct AckData(DashMap<U256, DataType>);

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

    let mut sender_cache = Arc::new(ChunkSended::default());
    let sended_data_clone = Arc::clone(&sender_cache);

    let ack_data_clone = Arc::clone(&Arc::new(AckData(DashMap::new())));

    thread::spawn(move || {
        let mut buff = [0u8; 65_535];

        // --- LISTNER ---
        loop {
            if let Ok((amt, src)) = socket_clone.recv_from(&mut buff) {
                let mut actual_byte = &buff[..amt];

                if let Ok(msg) = Chunk::deserialze(&mut actual_byte) {
                    if msg.msg_type == MsgType::Ack {
                        let mut chunk = ack_data_clone
                            .0
                            .entry(msg.id)
                            .and_modify(|data| {
                                data.ack
                                    .entry(msg.index)
                                    .and_modify(|val| val.push(src))
                                    .or_insert_with(|| vec![src]);
                            })
                            .or_insert_with(|| {
                                let mut new = BTreeMap::new();
                                new.insert(msg.index, vec![src]);

                                DataType {
                                    ack: new,
                                    removed_ack: 0,
                                }
                            });

                        if chunk.ack.len() == (msg.total_chunks - chunk.removed_ack) as usize {
                            let target_peer_len: usize = if chunk.ack.len() == 0 {
                                sended_data_clone.0.remove(&msg.id).unwrap().1.peers.len()
                            } else {
                                sended_data_clone.0.get(&msg.id).unwrap().peers.len()
                            };

                            let DataType { ack, removed_ack } = &mut *chunk;

                            ack.retain(|_k, v| {
                                if v.len() == target_peer_len {
                                    *removed_ack += 1;
                                    false
                                } else {
                                    true
                                }
                            });
                        } else {
                            return;
                        }
                    } else {
                        let mut nodes_list = list_clone.lock().unwrap();
                        if !nodes_list.tracker.contains(&src) {
                            nodes_list.tracker.push(src);
                            nodes_list.combined_list.push(src);
                        }
                    }
                }
            }
        }
    });

    // -- SENDER --
    loop {
        let data = 'node_list: {
            let list = node_list.lock().unwrap();

            if list.tracker.is_empty() {
                break 'node_list None;
            }

            if list.combined_list.len() < 1 {
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

        println!("List sended to:- {:?}", send_to_peers);
        let cache = Arc::get_mut(&mut sender_cache).unwrap();

        match cache.send_in_chunks(MsgType::Peers, &buffer, None, None, &send_to_peers, &socket) {
            Ok(_msg_id) => {
                let mut list = node_list.lock().unwrap();
                list.tracker.retain(|&node| !send_to_peers.contains(&node));
            }
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
