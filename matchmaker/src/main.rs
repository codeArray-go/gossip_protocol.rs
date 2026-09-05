use std::collections::HashMap;
use std::net::UdpSocket;

fn main() {
    let socket = UdpSocket::bind("0.0.0.0:9000").expect("Failed to bind Matchmacker");
    println!("Matchmaker is open on port: 9000");

    let mut activ_peer = HashMap::new();
    let mut buffer = [0u8; 1024];

    loop {
        if let Ok((amt, src_add)) = socket.recv_from(&mut buffer) {
            activ_peer.insert(10, src_add);
        }
    }
}
