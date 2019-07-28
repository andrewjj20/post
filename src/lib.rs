#[macro_use]
extern crate futures;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;

pub mod find_service;
mod framing;
pub mod publisher;
pub mod subscriber;

use std::fmt;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time;
use tokio::net::UdpSocket;

type SendError = tokio::sync::mpsc::error::SendError;

#[derive(Debug)]
pub enum Error {
    Empty,
    AddrParseError,
    IoError(io::Error),
    FindServiceError(find_service::ServerError),
    FramingError(framing::Error),
    SendError(SendError),
    TimerError(tokio::timer::Error),
}

type DataGram = (framing::Message, SocketAddr);
type Generation = u64;
pub const MAX_DATA_SIZE: usize = 1024;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            Error::Empty => write!(f, "Empty Error"),
            Error::IoError(e) => write!(f, "IO Error: {}", e),
            Error::FindServiceError(e) => write!(f, "Find Service Error: {}", e),
            Error::FramingError(e) => write!(f, "Framing Error: {}", e),
            Error::SendError(e) => write!(f, "Internal Stream Error: {}", e),
            Error::AddrParseError => write!(f, "Error Parsing Address"),
            Error::TimerError(e) => write!(f, "Error in tokio timer: {}", e),
        }
    }
}

impl From<()> for Error {
    fn from(_err: ()) -> Error {
        Error::Empty
    }
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

impl From<framing::Error> for Error {
    fn from(err: framing::Error) -> Error {
        Error::FramingError(err)
    }
}

impl From<SendError> for Error {
    fn from(err: SendError) -> Error {
        Error::SendError(err)
    }
}

impl From<tokio::timer::Error> for Error {
    fn from(err: tokio::timer::Error) -> Error {
        Error::TimerError(err)
    }
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PublisherDesc {
    pub name: String,
    pub host_name: String,
    pub port: u16,
    pub subscriber_expiration: time::Duration,
}

impl PublisherDesc {
    fn to_tokio_socket(&self) -> Result<UdpSocket> {
        let addr = match self.to_socket_addrs()?.next() {
            Some(addr) => addr,
            None => {
                return Err(Error::AddrParseError);
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

impl fmt::Display for PublisherDesc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "name: {}, host_name: {}, port: {}",
            self.name, self.host_name, self.port
        )
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ConnectionInfo {
    pub last_report: time::SystemTime,
    pub expiration: time::SystemTime,
}

#[cfg(test)]
mod tests {}
