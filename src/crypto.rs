use k256::{ Secp256k1, elliptic_curve::Generate };
use serde::{ Deserialize, Serialize };
use ecdsa::{ SigningKey, Signature as ECDSASignature };

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Signature(ECDSASignature<Secp256k1>);

#[derive(Deserialize, Clone)]
pub struct PrivateKey(#[serde(with = "signin_key")] pub SigningKey<Secp256k1>);
mod signin_key {
    use serde::Deserialize;
    pub fn serialize<S>(
        serializer: S,
        key: &super::SigningKey<super::Secp256k1>
    ) -> Result<S::Ok, S::Error>
        where S: serde::Serializer
    {
        serializer.serialize_bytes(&key.to_bytes())
    }

    pub fn deserialize<'de, D>(
        deserializer: D
    ) -> Result<super::SigningKey<super::Secp256k1>, D::Error>
        where D: serde::Deserializer<'de>
    {
        let bytes: Vec<u8> = Vec::<u8>::deserialize(deserializer)?;
        Ok(super::SigningKey::from_slice(&bytes).unwrap())
    }
}

impl PrivateKey {
    pub fn new() -> Self {
        PrivateKey(SigningKey::generate())
    }
}
