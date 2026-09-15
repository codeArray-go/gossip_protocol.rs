use uint::construct_uint;

pub mod byte_converter;
pub mod hash;
pub mod utils;

construct_uint!(
    pub struct U256(4);
);
