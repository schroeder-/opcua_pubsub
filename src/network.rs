// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2020 Alexander Schrode
use opcua_types::status_code::StatusCode;
use socket2::{Domain, Protocol, Socket, Type};
use std::io;
use std::mem::MaybeUninit;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use log::error;

/// Uadp message configuration which is used to receive/send udp messages 
pub struct UadpNetworkConnection {
    send_socket: Socket,
    addr2: socket2::SockAddr,
    addr: SocketAddr,
}

pub struct UadpNetworkReceiver{
    recv_socket: Socket,
}

impl UadpNetworkReceiver{
    pub fn receive_msg(&self) -> Result<Vec::<u8>, StatusCode>{
        let mut buf = [MaybeUninit::new(0u8); 16000];
        match self.recv_socket.recv_from(&mut buf) {
            Ok((len, _remote_addr)) => {
                let data = unsafe { assume_init(&buf[..len]) };
                Ok(data.to_vec())
            }
            Err(e) => {
                error!("Error reading udp socket {:}", e);
                Err(StatusCode::BadCommunicationError)
            }
        }
    }
    
}

impl UadpNetworkConnection {
    /// creates a new instance from ip:port string
    pub fn new(url: &String) -> std::io::Result<Self> {
        let addr = match SocketAddr::from_str(url) {
            Err(e) => {
                error!("Uadp url: {} is not valid! {:}", url, e);
                return Err(std::io::Error::from(std::io::ErrorKind::NotFound));
            }
            Ok(a) => a,
        };
        let send_socket = new_sender(&addr)?;
        Ok(UadpNetworkConnection { 
            send_socket, addr: addr, addr2: socket2::SockAddr::from(addr)})
    }

    // creates a receiver for udp messages
    pub fn create_receiver(&self) -> std::io::Result<UadpNetworkReceiver>{
        let recv_socket =  join_multicast(self.addr)?;
        Ok(UadpNetworkReceiver{recv_socket})
    }
    /// sends a multicast message
    pub fn send(&self, b: &[u8]) -> io::Result<usize>{
        self.send_socket.send_to(b, &self.addr2)
    }
}


/// On Windows, unlike all Unix variants, it is improper to bind to the multicast address
///
/// see https://msdn.microsoft.com/en-us/library/windows/desktop/ms737550(v=vs.85).aspx
#[cfg(windows)]
fn bind_multicast(socket: &Socket, addr: &SocketAddr) -> io::Result<()> {
    socket.set_reuse_address(true)?;
    socket.set_broadcast(true)?;
    let addr = match *addr {
        SocketAddr::V4(addr) => SocketAddr::new(Ipv4Addr::new(0, 0, 0, 0).into(), addr.port()),
        SocketAddr::V6(addr) => {
            SocketAddr::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0).into(), addr.port())
        }
    };
    socket.bind(&socket2::SockAddr::from(addr))
}

// On unixes we bind to the multicast address, which causes multicast packets to be filtered
#[cfg(unix)]
fn bind_multicast(socket: &Socket, addr: &SocketAddr) -> io::Result<()> {
    socket.set_broadcast(true)?;
    socket.bind(&socket2::SockAddr::from(*addr))
}

/// creates a new socket depending of ip4 or ipv6
fn new_socket(addr: &SocketAddr) -> std::io::Result<Socket> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };

    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    Ok(socket)
}

/// creates a udp listener for multicast messages
fn join_multicast(addr: SocketAddr) -> io::Result<Socket> {
    let ip_addr = addr.ip();

    let socket = new_socket(&addr)?;
    // depending on the IP protocol we have slightly different work
    match ip_addr {
        IpAddr::V4(ref mdns_v4) => {
            // join to the multicast address, with all interfaces
            socket.join_multicast_v4(mdns_v4, &Ipv4Addr::new(0, 0, 0, 0))?;
        }
        IpAddr::V6(ref mdns_v6) => {
            // join to the multicast address, with all interfaces (ipv6 uses indexes not addresses)
            socket.join_multicast_v6(mdns_v6, 0)?;
            socket.set_only_v6(true)?;
        }
    };

    // bind us to the socket address.
    bind_multicast(&socket, &addr)?;
    Ok(socket)
}

/// Creates new socket to send multicast messages
pub fn new_sender(addr: &SocketAddr) -> io::Result<Socket> {
    let socket = new_socket(addr)?;
    if addr.is_ipv4() {
        socket.set_multicast_if_v4(&Ipv4Addr::new(0, 0, 0, 0))?;
    } else {
        //@TODO find correct v6 
        socket.set_multicast_if_v6(0)?;
    }
    Ok(socket)
}

/// Assume the `buf`fer to be initialised.
// TODO: replace with `MaybeUninit::slice_assume_init_ref` once stable.
unsafe fn assume_init(buf: &[MaybeUninit<u8>]) -> &[u8] {
    &*(buf as *const [MaybeUninit<u8>] as *const [u8])
}
