#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate futures;
extern crate http;
extern crate hyper;
extern crate serde;
extern crate serde_json;
extern crate tokio;
#[macro_use]
extern crate log;

pub mod find_service;
mod framing;

use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time;
use tokio::net::UdpSocket;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PublisherDesc {
    pub name: String,
    pub host_name: String,
    pub port: u16,
}

impl<'a> ToSocketAddrs for PublisherDesc {
    type Iter = std::vec::IntoIter<SocketAddr>;
    fn to_socket_addrs(&self) -> io::Result<Self::Iter> {
        (self.host_name.as_str(), self.port).to_socket_addrs()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ConnectionInfo {
    pub last_report: time::SystemTime,
}

pub enum PublisherState {
    Init,
    Active,
    Closed,
    Error,
}

pub struct Publisher {
    desc: PublisherDesc,
    state: PublisherState,
    socket: UdpSocket,
}

impl Publisher {
    pub fn new(name: String, host_name: String, port: u16) -> io::Result<Publisher> {
        let desc = PublisherDesc {
            name,
            host_name,
            port,
        };
        let addr = match desc.to_socket_addrs()?.next() {
            Some(addr) => addr,
            None => return Err(io::Error::new(io::ErrorKind::Other, "Address not provided")),
        };
        let socket = UdpSocket::bind(&addr)?;
        Ok(Publisher {
            desc,
            state: PublisherState::Init,
            socket,
        })
    }

    pub fn activate(&mut self) -> io::Result<&mut Publisher> {
        Ok(self)
    }

    pub fn is_active(&self) -> bool {
        match &self.state {
            PublisherState::Active => true,
            _ => false,
        }
    }
}

pub enum SubscriptionState {
    Active,
    Closed,
    Error,
}

pub struct Subscription {
    desc: PublisherDesc,
    state: SubscriptionState,
    socket: UdpSocket,
}

#[cfg(test)]
mod tests {}
