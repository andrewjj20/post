#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;

pub mod find_service;
mod framing;
pub mod publisher;
pub mod subscriber;

use find_service::proto;
use std::fmt;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time;
use tokio::{net::UdpSocket, sync::oneshot::error::RecvError as OneshotRecvError, time as timer};

#[derive(Debug)]
pub enum Error {
    Empty,
    AddrParseError,
    IoError(io::Error),
    FramingError(framing::Error),
    TimerError(timer::Error),
    OneshotError(OneshotRecvError),
}

type DataGram = (framing::Message, SocketAddr);
type Generation = u64;
pub const MAX_DATA_SIZE: usize = 1024;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            Error::Empty => write!(f, "Empty Error"),
            Error::IoError(e) => write!(f, "IO Error: {}", e),
            Error::FramingError(e) => write!(f, "Framing Error: {}", e),
            Error::AddrParseError => write!(f, "Error Parsing Address"),
            Error::TimerError(e) => write!(f, "Error in tokio timer: {}", e),
            Error::OneshotError(e) => write!(f, "Error in internal messaging:{}", e),
        }
    }
}

impl std::error::Error for Error {}

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

impl From<framing::Error> for Error {
    fn from(err: framing::Error) -> Error {
        Error::FramingError(err)
    }
}

impl From<timer::Error> for Error {
    fn from(err: timer::Error) -> Error {
        Error::TimerError(err)
    }
}

impl From<OneshotRecvError> for Error {
    fn from(err: OneshotRecvError) -> Error {
        Error::OneshotError(err)
    }
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PublisherDesc {
    pub name: String,
    pub host_name: String,
    pub port: u16,
    pub subscriber_expiration_interval: time::Duration,
}

impl PublisherDesc {
    async fn to_tokio_socket(&self) -> Result<UdpSocket> {
        let addr = match self.to_socket_addrs()?.next() {
            Some(addr) => addr,
            None => {
                return Err(Error::AddrParseError);
            }
        };
        Ok(UdpSocket::bind(&addr).await?)
    }
}

#[derive(Debug)]
pub enum PublisherConversionError {
    Time(proto::TimeError),
    NoExpiration,
}

impl std::fmt::Display for PublisherConversionError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "Publisher Conversion Error: {:?}", self)
    }
}

impl std::error::Error for PublisherConversionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl std::convert::From<proto::TimeError> for PublisherConversionError {
    fn from(error: proto::TimeError) -> Self {
        PublisherConversionError::Time(error)
    }
}

impl std::convert::TryFrom<proto::PublisherDesc> for PublisherDesc {
    type Error = PublisherConversionError;
    fn try_from(proto_value: proto::PublisherDesc) -> std::result::Result<Self, Self::Error> {
        let proto::PublisherDesc {
            name,
            host_name,
            port: port32,
            subscriber_expiration_interval: proto_interval,
        } = proto_value;
        let subscriber_expiration_interval: time::Duration = match proto_interval {
            Some(expiration) => expiration.into(),
            None => return Err(PublisherConversionError::NoExpiration),
        };
        let port: u16 = port32 as u16;
        Ok(Self {
            name,
            host_name,
            port,
            subscriber_expiration_interval,
        })
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
