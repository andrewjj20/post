use std::net::SocketAddr;
use std::result;

#[derive(Debug)]
pub struct Error {
    description: String,
}
impl Error {
    pub fn new(description: String) -> Error {
        Error {
            description: description,
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub struct BaseMsg {
    addr: SocketAddr,
    version: u16,
    msg_type: u16,
    flags: u32,
}

#[derive(Debug)]
pub struct DataMsg {
    base: BaseMsg,
    generation: u64,
    start: usize,
    packet_size: usize,
    complete_size: usize,
}

#[derive(Debug)]
pub enum Request {
    Subscribe(BaseMsg),
    Unsubscribe(BaseMsg),
}

#[derive(Debug)]
pub enum Message {
    Data(DataMsg),
    Request(Request),
}

impl Message {
    pub fn parse(_addr: SocketAddr, _buf: &[u8], _size: usize) -> Result<Message> {
        Err(Error::new(String::from("Not Implemented")))
    }
}
