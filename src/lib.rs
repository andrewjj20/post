#![feature(rust_2018_preview)]
#![feature(nll)]

#[macro_use]
extern crate futures;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;

pub mod find_service;
mod framing;
pub mod publisher;

use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time;
use tokio::net::UdpSocket;

#[derive(Debug)]
pub enum Error {
    Empty,
    IoError(io::Error),
    FindServiceError(find_service::ServerError),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IoError(err)
    }
}

impl From<find_service::ServerError> for Error {
    fn from(err: find_service::ServerError) -> Error {
        Error::FindServiceError(err)
    }
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PublisherDesc {
    pub name: String,
    pub host_name: String,
    pub port: u16,
}

impl PublisherDesc {
    fn to_tokio_socket(&self) -> Result<UdpSocket> {
        let addr = match self.to_socket_addrs()?.next() {
            Some(addr) => addr,
            None => {
                return Err(Error::from(io::Error::new(
                    io::ErrorKind::Other,
                    "Address not provided",
                )))
            }
        };
        return Ok(UdpSocket::bind(&addr)?);
    }
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
