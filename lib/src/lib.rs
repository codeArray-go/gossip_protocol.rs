use std::{collections::HashSet, net::SocketAddr};

use uint::construct_uint;

pub mod byte_converter;
pub mod hash;
pub mod utils;

#[derive(Debug)]
pub enum NetworkManager {
    Text(String),
    SharedPeers(HashSet<SocketAddr>),
    RawData(Vec<u8>),
}

construct_uint!(
    pub struct U256(4);
);
