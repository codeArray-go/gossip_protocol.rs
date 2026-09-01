use std::collections::HashMap;
use std::net::UdpSocket;
use std::time::{Duration, Instant};

fn main() {
    // Server hamesha ek fixed public IP/Port par chalega
    let socket = UdpSocket::bind("0.0.0.0:9000").expect("Failed to bind Matchmaker");
    println!("Matchmaker Server listening on port 9000...");

    // RAM me peers ko track karne ke liye. (IP:Port -> Last seen time)
    let mut active_peers = HashMap::new();
    let mut buffer = [0u8; 1024];

    loop {
        // 1. Packet receive karo
        let (amt, src_addr) = match socket.recv_from(&mut buffer) {
            Ok(result) => result,
            Err(_) => continue,
        };

        // 2. Dead peers ko hatao (Jin hone 30 second se ping nahi kiya)
        let now = Instant::now();
        active_peers.retain(|_, last_seen| now.duration_since(*last_seen) < Duration::from_secs(30));

        let payload = &buffer[..amt];
        if payload.is_empty() { continue; }

        // 3. Protocol Logic
        match payload[0] {
            1 => {
                // COMMAND 1: REGISTER
                // Peer ne kaha "I am alive". Humne uska IP aur time note kar liya.
                active_peers.insert(src_addr, now);
                println!("Registered Peer: {}", src_addr);
            }
            2 => {
                // COMMAND 2: GET_PEERS
                // Peer ko baki sabhi active peers ki list chahiye
                active_peers.insert(src_addr, now); // Request aayi toh active toh hai hi

                // Sabhi dusre peers ke IP:Port ko string me comma-separated bana lo
                // (High performance ke liye aap ise raw 6-byte binary me bhi bhej sakte hain)
                let peer_list: Vec<String> = active_peers
                    .keys()
                    .filter(|&&addr| addr != src_addr) // Khud ka IP wapas mat bhejo
                    .map(|addr| addr.to_string())
                    .collect();

                let response_str = peer_list.join(",");
                
                // Response command '3' ke sath bhejo
                let mut response = vec![3]; 
                response.extend_from_slice(response_str.as_bytes());

                socket.send_to(&response, src_addr).expect("Failed to send peer list");
                println!("Sent {} peers to {}", peer_list.len(), src_addr);
            }
            _ => {
                println!("Unknown command from {}", src_addr);
            }
        }
    }
}