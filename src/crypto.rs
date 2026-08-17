use k256::{ Secp256k1, elliptic_curve::Generate };
use ecdsa::SigningKey;

#[derive(Debug, Clone)]
pub struct PrivateKey(pub SigningKey<Secp256k1>);

impl PrivateKey {
    pub fn new() -> Self {
        PrivateKey(SigningKey::generate())
    }
}
