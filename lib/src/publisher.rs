use super::find_service;
use super::framing;
use super::framing::{Acknowledgement, Message, MessageCodec, Request};
use super::{DataGram, Error, Generation, PublisherDesc, Result};
use futures::{
    future::TryFutureExt,
    sink::Sink,
    stream::{Stream, StreamExt},
};
use itertools::Itertools;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time;
use std::vec::Vec;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::mpsc::{self, Sender};
use tokio::{sync::Mutex, time as timer};
use tokio_util::udp::UdpFramed;

#[derive(Debug)]
struct Subscriber {
    addr: SocketAddr,
    expiration: time::SystemTime,
}

struct PublisherShared {
    subscribers: HashMap<SocketAddr, Subscriber>,
    is_active: bool,
    subscriber_expiration_interval: time::Duration,
}

impl PublisherShared {
    fn handle_subscription(&mut self, addr: SocketAddr) -> Option<DataGram> {
        let timestamp = time::SystemTime::now();
        let message =
            Message::Acknowledgement(Acknowledgement::Subscription(framing::Subscription {
                timeout_interval: self.subscriber_expiration_interval,
            }));
        let expiration = timestamp + self.subscriber_expiration_interval;
        match self.subscribers.get_mut(&addr) {
            Some(v) => {
                v.expiration = timestamp + self.subscriber_expiration_interval;
                debug!("Subscription Renewed {:?}", v);
            }
            None => {
                let sub = Subscriber { addr, expiration };
                info!("Subscribe {:?}", sub);
                self.subscribers.insert(addr, sub);
            }
        };
        Some((message, addr))
    }
    fn handle_unsubscribe(&mut self, addr: &SocketAddr) -> Option<DataGram> {
        info!("Unsubscribe {}", addr);
        self.subscribers.remove(addr);
        None
    }
    fn prune_stale_subscriptions(&mut self, timestamp: time::SystemTime) {
        self.subscribers.retain(|_k, v| {
            let result = v.expiration >= timestamp;

            if !result {
                info!("Subscriber Timed out: {:?}", v);
            }

            result
        });
    }
}

type ProtectedShared = Arc<Mutex<PublisherShared>>;

async fn handle_publisher_backend(
    shared: Arc<Mutex<PublisherShared>>,
    incomming: DataGram,
) -> Option<DataGram> {
    debug!("Message received: {}", incomming.0);
    let ret = match incomming.0 {
        Message::Request(r) => match r {
            Request::Subscribe(_) => shared.lock().await.handle_subscription(incomming.1),
            Request::Unsubscribe(_) => shared.lock().await.handle_unsubscribe(&incomming.1),
        },
        Message::Data(_) => {
            error!("Data Message sent to publisher from {}", incomming.1);
            None
        }
        _ => {
            error!(
                "Unhandled Message received from {} :{}",
                incomming.1, incomming.0
            );
            None
        }
    };
    if let Some(dgram) = &ret {
        debug!("Response: {:?}", dgram);
    }
    ret
}

fn log_err<T>(e: T)
where
    T: std::fmt::Display,
{
    error!("Registration Error {}", e);
}

fn stop_stream_when_inactive<'a, T, I>(
    stream: T,
    shared: ProtectedShared,
) -> impl Stream<Item = I> + 'a
where
    T: Stream<Item = I> + 'a,
{
    stream.take_while(move |_| {
        let loc_shared = shared.clone();
        async move { loc_shared.lock().await.is_active }
    })
}

fn publisher_registration(
    desc: PublisherDesc,
    find_uri: String,
    shared: Arc<Mutex<PublisherShared>>,
) {
    let reg_info = Arc::new((find_uri, desc));
    tokio::spawn(async move {
        let interval = time::Duration::new(0, 0);
        let fold_reg_info = Arc::clone(&reg_info);
        timer::delay_for(interval).await;
        if shared.lock().await.is_active {
            let (uri, desc) = &*Arc::clone(&fold_reg_info);
            let result = find_service::publisher_register(uri.clone(), desc.clone())
                .map_err(log_err)
                .await;
            Some(match result {
                Ok(resp) => ((), resp.expiration_interval / 2),
                Err(_) => ((), interval),
            })
        } else {
            None
        }
    });
}

pub struct Publisher {
    shared: ProtectedShared,
    sink: Sender<DataGram>,
    generation: Generation,
    in_poll: Option<Pin<Box<(dyn Future<Output = Result<()>>)>>>,
}

impl Publisher {
    pub async fn new(
        name: String,
        host_name: String,
        port: u16,
        subscriber_expiration_interval: time::Duration,
        find_uri: String,
    ) -> Result<Publisher> {
        let desc = PublisherDesc {
            name,
            host_name,
            port,
            subscriber_expiration_interval,
        };
        Publisher::from_description(desc, find_uri).await
    }

    pub async fn from_description(desc: PublisherDesc, find_uri: String) -> Result<Publisher> {
        let subscriber_expiration_interval = desc.subscriber_expiration_interval;
        let shared = Arc::new(Mutex::new(PublisherShared {
            subscribers: HashMap::new(),
            is_active: true,
            subscriber_expiration_interval,
        }));
        let (udp_sink, udp_stream) =
            UdpFramed::new(desc.to_tokio_socket().await?, MessageCodec {}).split();
        let (sink, stream) = mpsc::channel::<DataGram>(1);

        {
            let reserved_shared = Arc::clone(&shared);
            let backend_sink = sink.clone();
            tokio::spawn(
                stop_stream_when_inactive(
                    udp_stream.filter_map(async move |r| match r {
                        Err(e) => {
                            error!("Stream Error {}", e);
                            None
                        }
                        Ok(ret) => Some(ret),
                    }),
                    Arc::clone(&shared),
                )
                .for_each(move |incomming| {
                    let shared = reserved_shared.clone();
                    let mut sink = backend_sink.clone();
                    async move {
                        if let Some(msg) = handle_publisher_backend(shared, incomming).await {
                            if sink.send(msg).await.is_err() {
                                error!("Broken pipe when processing output");
                            }
                        };
                    }
                }),
            );
        }
        tokio::spawn(stream.map(Ok).forward(udp_sink).map_err(|e| {
            error!("Sink Error {}", e);
        }));

        publisher_registration(desc, find_uri, Arc::clone(&shared));

        Ok(Publisher {
            shared,
            sink,
            generation: 1,
            in_poll: None,
        })
    }

    fn do_send(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        let ret = if let Some(future) = self.in_poll.as_mut() {
            future.as_mut().poll(cx)
        } else {
            Poll::Ready(Ok(()))
        };
        if let Poll::Ready(_) = ret {
            self.in_poll = None;
        }
        ret
    }

    fn flush(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        loop {
            futures::ready!(self.do_send(cx))?;
            if self.in_poll.is_none() {
                return Poll::Ready(Ok(()));
            }
        }
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        futures::executor::block_on(self.shared.lock()).is_active = false;
    }
}

impl Sink<Vec<u8>> for Publisher {
    type Error = Error;

    fn start_send(self: Pin<&mut Self>, item: Vec<u8>) -> Result<()> {
        if self.in_poll.is_some() {
            return Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "Started Send without ready",
            )));
        }
        let pin = self.get_mut();

        let timestamp = time::SystemTime::now();
        let generation = pin.generation;
        let shared = pin.shared.clone();
        let mut sink = pin.sink.clone();
        pin.in_poll = Some(Box::pin(async move {
            let dgrams = {
                let mut shared = shared.lock().await;
                shared.prune_stale_subscriptions(timestamp);
                Vec::from_iter(
                    Message::split_data_msgs(item.as_slice(), generation)?
                        .into_iter()
                        .cartesian_product(shared.subscribers.values().map(|s| s.addr)),
                )
            };
            for dgram in dgrams {
                sink.send(dgram).await.map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Publisher pipe ended")
                })?;
            }
            Ok(())
        }));
        pin.generation += 1;

        Ok(())
    }

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        self.get_mut().flush(cx)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        self.get_mut().flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let pin = self.get_mut();
        let ret = pin.flush(cx);
        futures::executor::block_on(pin.shared.lock()).is_active = false;
        ret
    }
}

#[cfg(test)]
mod tests {}
