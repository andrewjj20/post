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

/// Future that comletes when the subscription has been acknoledge to the subscriber.
pub struct SubscriptionComplete<'a> {
    inner: &'a mut Subscription,
}

impl<'a> std::future::Future for SubscriptionComplete<'a> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        debug!("Poll for subscription complete");
        let pin = self.get_mut();
        loop {
            futures::ready!(pin.inner.handle_message(cx));
            debug!("subscribed: {}", pin.inner.subscribed);
            if pin.inner.subscribed {
                debug!("SubscriptionComplete completed");
                break Poll::Ready(());
            }
            debug!("SubscriptionComplete Retry");
        }
    }
}

pub struct Subscription {
    desc: PublisherDesc,
    addr: SocketAddr,
    inner_stream: Pin<Box<dyn Send + Stream<Item = Result<DataGram>>>>,
    sink: Sender<DataGram>,
    generation: Generation,
    current: std::option::Option<BytesMut>,
    chunks: usize,
    received_chunks: HashSet<usize>,
    subscribed: bool,
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
            subscribed: false,
        })
    }

    pub fn description(&self) -> &PublisherDesc {
        &self.desc
    }

    pub fn wait_for_subscrition_complete(&mut self) -> SubscriptionComplete {
        SubscriptionComplete { inner: self }
    }

    fn handle_message(&mut self, cx: &mut Context) -> Poll<()> {
        loop {
            debug!("Receive one message");
            let message = match futures::ready!(self.inner_stream.as_mut().poll_next(cx)) {
                Some(Ok(m)) => m,
                Some(Err(err)) => {
                    error!("Error parsing message {}", err);
                    continue;
                }
                None => continue,
            };

            debug!("message {:?}", message);
            if message.1 == self.addr {
                match message.0 {
                    Message::Data(data) => {
                        if data.generation > self.generation {
                            debug!("New data, clearing old");
                            self.generation = data.generation;
                            let completed = data.complete_size;
                            let mut new_current = BytesMut::new();
                            new_current.resize(completed, 0);
                            self.current.replace(new_current);
                            self.chunks = completed / MAX_DATA_SIZE
                                + if completed % MAX_DATA_SIZE == 0 { 0 } else { 1 };
                            self.received_chunks.clear();
                        }
                        if data.generation == self.generation {
                            if let Some(current) = &mut self.current {
                                if self.received_chunks.insert(data.chunk) {
                                    let offset = data.chunk * MAX_DATA_SIZE;
                                    debug!("current length is: {}", current.len());
                                    current[offset..data.data.len()].copy_from_slice(&data.data);
                                }
                            }
                        }

                        if self.received_chunks.len() == self.chunks {
                            return Poll::Ready(());
                        }
                    }
                    Message::Acknowledgement(ack) => {
                        debug!("Ack: {:?}", ack);
                        match ack {
                            Acknowledgement::Subscription(sub) => {
                                debug!("Subscription Ack: {}", sub);
                                self.subscribed = true;
                                let timeout = sub.timeout_interval / 2;
                                let mut sink = self.sink.clone();
                                let addr = self.addr;
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
                                return Poll::Ready(());
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

impl Stream for Subscription {
    type Item = Bytes;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let pin = self.get_mut();
        loop {
            futures::ready!(pin.handle_message(cx));
            if pin.received_chunks.len() == pin.chunks {
                if let Some(current) = pin.current.take() {
                    return Poll::Ready(Some(current.freeze()));
                }
            }
        }
    }
}
