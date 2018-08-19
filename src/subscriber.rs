use super::framing::{BaseMsg, Message, MessageCodec, Request};
use super::{DataGram, Error, Generation, PublisherDesc, Result, MAX_DATA_SIZE};
use futures::{
    future::Future,
    stream::{self, SplitStream},
    sync::mpsc::{self, Sender},
    Async, Poll, Sink, Stream,
};
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use tokio::net::{UdpFramed, UdpSocket};

pub struct Subscription {
    desc: PublisherDesc,
    addr: SocketAddr,
    inner_stream: SplitStream<UdpFramed<MessageCodec>>,
    _sink: Sender<DataGram>,
    generation: Generation,
    current: Vec<u8>,
    chunks: usize,
    received_chunks: HashSet<usize>,
}

impl Subscription {
    pub fn new(desc: PublisherDesc) -> Result<Subscription> {
        let addr = match try!(desc.to_socket_addrs()).next() {
            Some(a) => a,
            None => return Err(Error::AddrParseError),
        };

        let bind_addr = SocketAddr::new(
            match addr {
                SocketAddr::V4(_) => IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
                SocketAddr::V6(_) => IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0)),
            },
            0,
        );
        let (udp_sink, udp_stream) =
            UdpFramed::new(try!(UdpSocket::bind(&bind_addr)), MessageCodec {}).split();

        let (sender, receiver) = mpsc::channel(1);

        tokio::spawn(
            stream::once::<DataGram, Error>(Ok((
                Message::Request(Request::Subscribe(BaseMsg {})),
                addr,
            ))).forward(sender.clone())
                .map(|_| ())
                .map_err(|_| ()),
        );

        {
            let addr_moveable = addr.clone();
            tokio::spawn(
                receiver
                    .map_err(|_| Error::Empty)
                    .forward(udp_sink)
                    .and_then(move |streams| {
                        let (_, sink) = streams;
                        sink.send((
                            Message::Request(Request::Unsubscribe(BaseMsg {})),
                            addr_moveable,
                        )).map(|_| ())
                    })
                    .map_err(|e| {
                        error!("Sink Error {}", e);
                    }),
            );
        }
        Ok(Subscription {
            desc,
            addr,
            inner_stream: udp_stream,
            _sink: sender,
            generation: 0,
            current: Vec::new(),
            chunks: 0,
            received_chunks: HashSet::new(),
        })
    }

    pub fn description(&self) -> &PublisherDesc {
        &self.desc
    }
}

impl Stream for Subscription {
    type Item = Vec<u8>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            let message = match try_ready!(self.inner_stream.poll()) {
                Some(m) => m,
                None => return Ok(Async::Ready(None)),
            };

            if message.1 == self.addr {
                if let Message::Data(data) = message.0 {
                    if data.generation > self.generation {
                        self.generation = data.generation;
                        let completed = data.complete_size;
                        self.current.resize(completed, 0);
                        self.chunks = completed / MAX_DATA_SIZE
                            + if completed % MAX_DATA_SIZE == 0 { 0 } else { 1 };
                        self.received_chunks.clear();
                    }
                    if data.generation == self.generation {
                        if self.received_chunks.insert(data.chunk) {
                            let offset = data.chunk * MAX_DATA_SIZE;
                            self.current.as_mut_slice()[offset..data.data.len()]
                                .copy_from_slice(data.data.as_slice());
                        }
                        if self.received_chunks.len() == self.chunks {
                            return Ok(Async::Ready(Some(self.current.clone())));
                        }
                    }
                }
            }
        }
    }
}
