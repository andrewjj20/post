use super::framing::{Acknowledgement, BaseMsg, Message, MessageCodec, Request};
use super::{DataGram, Error, Generation, PublisherDesc, Result, MAX_DATA_SIZE};
use bytes::{Bytes, BytesMut};
use futures::{
    sink::SinkExt,
    stream::{Stream, StreamExt},
    FutureExt,
};
use std::collections::HashSet;
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs},
    pin::Pin,
    task::{Context, Poll},
};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{self, Sender};
use tokio::time as timer;
use tokio_util::udp::UdpFramed;

pub struct Subscription {
    desc: PublisherDesc,
    addr: SocketAddr,
    inner_stream: Pin<Box<dyn Stream<Item = Result<DataGram>>>>,
    sink: Sender<DataGram>,
    generation: Generation,
    current: std::option::Option<BytesMut>,
    chunks: usize,
    received_chunks: HashSet<usize>,
}

impl Subscription {
    pub async fn new(desc: PublisherDesc) -> Result<Subscription> {
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
            UdpFramed::new(UdpSocket::bind(&bind_addr).await?, MessageCodec {}).split();

        let (sender, receiver) = mpsc::channel(10);

        {
            let mut subscribe_sender = sender.clone();
            tokio::spawn(async move {
                subscribe_sender
                    .send((Message::Request(Request::Subscribe(BaseMsg {})), addr))
                    .await
            });
        }

        {
            let mut pinned_sink = Box::pin(udp_sink);
            let addr_moveable = addr;
            tokio::spawn(async move {
                let ret = receiver
                    .map(|m| {
                        debug!("Sending Message");
                        Ok(m)
                    })
                    .forward(pinned_sink.as_mut())
                    .await;

                if let Err(err) = pinned_sink
                    .send((
                        Message::Request(Request::Unsubscribe(BaseMsg {})),
                        addr_moveable,
                    ))
                    .await
                {
                    error!("Unable to send final unsubscribe: {}", err);
                }

                ret
            });
        }
        Ok(Subscription {
            desc,
            addr,
            inner_stream: Box::pin(udp_stream),
            sink: sender,
            generation: 0,
            current: None,
            chunks: 0,
            received_chunks: HashSet::new(),
        })
    }

    pub fn description(&self) -> &PublisherDesc {
        &self.desc
    }
}

impl Stream for Subscription {
    type Item = Bytes;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let pin = self.get_mut();
        loop {
            let message = match futures::ready!(pin.inner_stream.as_mut().poll_next(cx)) {
                Some(Ok(m)) => m,
                Some(Err(err)) => {
                    error!("Error parsing message {}", err);
                    continue;
                }
                None => continue,
            };

            debug!("message {:?}", message);
            if message.1 == pin.addr {
                match message.0 {
                    Message::Data(data) => {
                        if data.generation > pin.generation {
                            pin.generation = data.generation;
                            let completed = data.complete_size;
                            let mut new_current = BytesMut::new();
                            new_current.resize(completed, 0);
                            pin.current.replace(new_current);
                            pin.chunks = completed / MAX_DATA_SIZE
                                + if completed % MAX_DATA_SIZE == 0 { 0 } else { 1 };
                            pin.received_chunks.clear();
                        }
                        if data.generation == pin.generation {
                            if let Some(current) = &mut pin.current {
                                if pin.received_chunks.insert(data.chunk) {
                                    let offset = data.chunk * MAX_DATA_SIZE;
                                    current[offset..data.data.len()].copy_from_slice(&data.data);
                                }
                            }
                            if pin.received_chunks.len() == pin.chunks {
                                if let Some(current) = pin.current.take() {
                                    return Poll::Ready(Some(current.freeze()));
                                }
                            }
                        }
                    }
                    Message::Acknowledgement(ack) => {
                        debug!("Ack: {:?}", ack);
                        match ack {
                            Acknowledgement::Subscription(sub) => {
                                debug!("Subscription Ack: {}", sub);
                                let timeout = sub.timeout_interval / 2;
                                let mut sink = pin.sink.clone();
                                let addr = pin.addr;
                                let resub = timer::delay_for(timeout).then(async move |_| {
                                    debug!("Sending Resubscription");
                                    match sink
                                        .send((
                                            Message::Request(Request::Subscribe(BaseMsg {})),
                                            addr,
                                        ))
                                        .await
                                    {
                                        Ok(()) => debug!("Sent Resubscription"),
                                        Err(_) => error!("Out put pipe shut unexpectedly"),
                                    };
                                });

                                tokio::spawn(resub);
                            }
                        }
                    }
                    _ => {
                        //Skip unknown
                        debug!("Unknown Message: {}", message.0);
                    }
                }
            }
        }
    }
}
