use super::framing::{Acknowledgement, BaseMsg, Message, MessageCodec, Request};
use super::{DataGram, Error, Generation, PublisherDesc, Result, MAX_DATA_SIZE};
use futures::{
    future::Future,
    stream::{self, SplitStream},
    Async, Poll, Sink, Stream,
};
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use std::time;
use tokio::net::{UdpFramed, UdpSocket};
use tokio::sync::mpsc::{self, Sender};
use tokio::timer;

pub struct Subscription {
    desc: PublisherDesc,
    addr: SocketAddr,
    inner_stream: SplitStream<UdpFramed<MessageCodec>>,
    sink: Sender<DataGram>,
    generation: Generation,
    current: Vec<u8>,
    chunks: usize,
    received_chunks: HashSet<usize>,
}

impl Subscription {
    pub fn new(desc: PublisherDesc) -> Result<Subscription> {
        let addr = match desc.to_socket_addrs()?.next() {
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
            UdpFramed::new(UdpSocket::bind(&bind_addr)?, MessageCodec {}).split();

        let (sender, receiver) = mpsc::channel(10);

        tokio::spawn(
            stream::once::<DataGram, Error>(Ok((
                Message::Request(Request::Subscribe(BaseMsg {})),
                addr,
            )))
            .forward(sender.clone())
            .map(|_| ())
            .map_err(|_| ()),
        );

        {
            let addr_moveable = addr.clone();
            tokio::spawn(
                receiver
                    .map_err(|_| Error::Empty)
                    .map(|m| {
                        debug!("Sending Message");
                        m
                    })
                    .forward(udp_sink)
                    .and_then(move |streams| {
                        let (_, sink) = streams;
                        sink.send((
                            Message::Request(Request::Unsubscribe(BaseMsg {})),
                            addr_moveable,
                        ))
                        .map(|_| ())
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
            sink: sender,
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
                match message.0 {
                    Message::Data(data) => {
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
                    Message::Acknowledgement(ack) => match ack {
                        Acknowledgement::Subscription(sub) => {
                            let timeout = sub.timeout_interval / 2;
                            let sink = self.sink.clone();
                            let addr = self.addr.clone();
                            let resub = timer::Delay::new(time::Instant::now() + timeout)
                                .map_err(|e| Error::from(e))
                                .and_then(move |_| {
                                    debug!("Sending Resubscription");
                                    sink.send((
                                        Message::Request(Request::Subscribe(BaseMsg {})),
                                        addr,
                                    ))
                                    .map_err(|e| Error::from(e))
                                })
                                .map(|_| {
                                    debug!("Sent Resubscription");
                                    ()
                                })
                                .map_err(|e| {
                                    error!("Issue sending resubscription: {}", e);
                                    ()
                                });

                            tokio::spawn(resub);
                        }
                    },
                    _ => {
                        //Skip unknown
                        debug!("Unknown Message: {}", message.0);
                    }
                }
            }
        }
    }
}
