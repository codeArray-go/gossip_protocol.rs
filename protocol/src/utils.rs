use std::{
    collections::HashSet,
    fs::File,
    io::{Error, Read, Result as IoResult, Write},
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    path::Path,
};

pub fn save_to_file<P: AsRef<Path>>(path: P, adds: &HashSet<SocketAddr>) -> IoResult<()> {
    let mut file = File::create(path)?;

    let len = adds.len() as u64;
    file.write_all(&len.to_le_bytes())?;

    for addr in adds {
        match addr {
            SocketAddr::V4(ipv4) => {
                file.write_all(&[0u8])?;
                file.write_all(&ipv4.ip().octets())?;
                file.write_all(&ipv4.port().to_le_bytes())?;
            }

            SocketAddr::V6(ipv6) => {
                file.write_all(&[1u8])?;
                file.write_all(&ipv6.ip().octets())?;
                file.write_all(&ipv6.port().to_le_bytes())?;
            }
        }
    }

    Ok(())
}

pub fn read_from_file(path: impl AsRef<Path>) -> IoResult<HashSet<SocketAddr>> {
    let mut file = File::open(path)?;

    let mut len_buf = [0u8; 8];
    file.read_exact(&mut len_buf)?;
    let len = u64::from_le_bytes(len_buf);

    let mut data: HashSet<SocketAddr> = HashSet::with_capacity(len as usize);

    for _ in 0..len {
        let mut tag = [0u8; 1];
        file.read_exact(&mut tag)?;

        let addr = match tag[0] {
            0 => {
                let mut ip = [0u8; 4];
                let mut port = [0u8; 2];

                file.read_exact(&mut ip)?;
                file.read_exact(&mut port)?;

                SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::from(ip),
                    u16::from_le_bytes(port),
                ))
            }
            1 => {
                let mut ip = [0u8; 16];
                let mut port = [0u8; 2];

                file.read_exact(&mut ip)?;
                file.read_exact(&mut port)?;

                SocketAddr::V6(SocketAddrV6::new(
                    Ipv6Addr::from(ip),
                    u16::from_le_bytes(port),
                    0,
                    0,
                ))
            }
            _ => {
                return Err(Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Corrupted file: Unknown IP tag.",
                ));
            }
        };

        data.insert(addr);
    }

    Ok(data)
}
