use std::{collections::HashSet, net::SocketAddr};

use uint::construct_uint;

pub mod byte_converter;
pub mod hash;
pub mod utils;

construct_uint!(
    pub struct U256(4);
);

#[derive(Debug, Clone)]
pub enum Message {
    Chat(String),
    TypeAck,
    ReRequest,
    SharedPeers(HashSet<SocketAddr>),
}
