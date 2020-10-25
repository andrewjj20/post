use super::Error as PubSubError;
use super::{Generation, MAX_DATA_SIZE};
use bytes::{Bytes, BytesMut};
use rmp_serde as rmps;
use rmp_serde::{decode::Error as DecodeError, encode::Error as EncodeError};
use std::fmt::Display;
use std::result;
use std::time::Duration;
use std::vec::Vec;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BaseMsg {}

impl Display for BaseMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> result::Result<(), std::fmt::Error> {
        write!(f, "")
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DataMsg {
    pub generation: Generation,
    pub chunk: usize,
    pub complete_size: usize,
    #[serde(with = "serde_bytes")]
    pub data: Bytes,
}

impl Display for DataMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> result::Result<(), std::fmt::Error> {
        write!(
            f,
            "Generation: {}, chunk: {}, len: {}, finished_len: {}",
            self.generation,
            self.chunk,
            self.data.len(),
            self.complete_size
        )
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Request {
    Subscribe(BaseMsg),
    Unsubscribe(BaseMsg),
}

impl Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> result::Result<(), std::fmt::Error> {
        match self {
            Request::Subscribe(b) => write!(f, "Subscription {}", b),
            Request::Unsubscribe(b) => write!(f, "Unsubscription {}", b),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Subscription {
    pub timeout_interval: Duration,
}

impl Display for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> result::Result<(), std::fmt::Error> {
        write!(
            f,
            "Subscription interval: {}.{:09}",
            self.timeout_interval.as_secs(),
            self.timeout_interval.subsec_nanos()
        )
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Acknowledgement {
    Subscription(Subscription),
}

impl Display for Acknowledgement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> result::Result<(), std::fmt::Error> {
        match self {
            Acknowledgement::Subscription(b) => write!(f, "Subscription {}", b),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Message {
    Data(DataMsg),
    Request(Request),
    Acknowledgement(Acknowledgement),
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> result::Result<(), std::fmt::Error> {
        match self {
            Message::Data(d) => write!(f, "Data {}", d),
            Message::Request(r) => write!(f, "Request {}", r),
            Message::Acknowledgement(r) => write!(f, "Acknowledgement {}", r),
        }
    }
}

impl Message {
    pub fn deserialize(buf: &[u8]) -> Result<Message, Error> {
        Ok(rmps::from_slice::<Message>(buf)?)
    }

    pub fn serialize(&self) -> result::Result<Vec<u8>, Error> {
        Ok(rmps::to_vec(&self)?)
    }

    pub fn split_data_msgs(mut buf: Bytes, generation: u64) -> result::Result<Vec<Message>, Error> {
        let mut ret: Vec<Message> = Vec::new();
        let mut chunk = 0;
        let total_len = buf.len();
        while !buf.is_empty() {
            let data = buf.split_to(std::cmp::min(buf.len(), MAX_DATA_SIZE));
            let message = Message::Data(DataMsg {
                generation,
                chunk,
                complete_size: total_len,
                data,
            });
            debug!("Queuing {}", message);
            ret.push(message);
            chunk += 1;
        }
        Ok(ret)
    }
}

#[derive(Debug)]
pub enum Error {
    Serialize(rmps::encode::Error),
    Deserialize(rmps::decode::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> result::Result<(), std::fmt::Error> {
        match self {
            Error::Serialize(e) => write!(f, "Serialize Error: {}", e),
            Error::Deserialize(e) => write!(f, "Deserialize Error: {}", e),
        }
    }
}
impl std::error::Error for Error {}

impl From<EncodeError> for Error {
    fn from(error: EncodeError) -> Error {
        Error::Serialize(error)
    }
}

impl From<DecodeError> for Error {
    fn from(error: DecodeError) -> Error {
        Error::Deserialize(error)
    }
}

pub struct MessageCodec {}

impl Encoder<Message> for MessageCodec {
    type Error = PubSubError;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(item.serialize()?.as_slice());
        Ok(())
    }
}

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = PubSubError;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        Ok(Some(Message::deserialize(src)?))
    }
}

#[cfg(test)]
mod tests {}
