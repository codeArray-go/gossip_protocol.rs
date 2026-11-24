use uint::construct_uint;

pub mod byte_converter;
pub mod hash;
pub mod utils;

construct_uint!(
    pub struct U256(4);
);

#[derive(Debug, Clone, PartialEq, Eq, Copy, Hash)]
#[repr(u8)]
pub enum MsgType {
    Chat = b'C',
    Ack = b'A',
    Re = b'R',
    Peers = b'L',
}

#[derive(Clone)]
pub enum Combined<'a> {
    Msg(&'a [u8]),
    Kind(&'a MsgType),
}
