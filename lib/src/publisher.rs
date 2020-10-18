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

/// Tracking information for [Subscriptions](super::subscriber::Subscription)
struct Subscriber {
    addr: SocketAddr,
    expiration: time::SystemTime,
}

/// [Publisher] state shared with a background management task
///
/// This is used by the [Publisher] and separate future managing background
/// [Subscription](super::subscriber::Subscription) management.
struct PublisherShared {
    subscribers: HashMap<SocketAddr, Subscriber>,
    is_active: bool,
    subscriber_expiration_interval: time::Duration,
}

impl PublisherShared {
    fn new(is_active: bool, subscriber_expiration_interval: time::Duration) -> Self {
        Self {
            subscribers: HashMap::new(),
            is_active,
            subscriber_expiration_interval,
        }
    }
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

/// The final type of PublishShared to ensure it is protected.
type ProtectedShared = Arc<Mutex<PublisherShared>>;

/// Handle the background management of a Publisher.
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

fn stop_stream_when_inactive<'a, S, I>(
    stream: S,
    shared: ProtectedShared,
) -> impl Stream<Item = I> + 'a
where
    S: Stream<Item = I> + 'a,
{
    stream.take_while(move |_| {
        let loc_shared = shared.clone();
        async move { loc_shared.lock().await.is_active }
    })
}

async fn publisher_registration(
    desc: PublisherDesc,
    client: find_service::Client,
    shared: Arc<Mutex<PublisherShared>>,
) ->Result<()> {
    let (reg_sender, reg_listener) = tokio::sync::oneshot::channel::<bool>();
    let reg_info = Arc::new((client, desc));
    tokio::spawn(async move {
        let mut reg_sender = Some(reg_sender);
        let interval = time::Duration::new(0, 0);
        let fold_reg_info = Arc::clone(&reg_info);
        timer::delay_for(interval).await;
        if shared.lock().await.is_active {
            let (client, desc) = &*Arc::clone(&fold_reg_info);
            let result = client
                .clone()
                .publisher_register(desc.clone())
                .map_err(log_err)
                .await;
            Some(match result {
                Ok(resp) => {
                    if let Some(sender) = reg_sender.take() {
                        //if the other side is not listening, we don't care
                        let _ = sender.send(true);
                    }
                    ((), resp.expiration_interval / 2)},
                Err(_) => ((), interval),
            })
        } else {
            None
        }
    });
    reg_listener.await?;
    Ok(())
}

/// Handles the distribution of messages to [Subscribers](super::subscriber::Subscription).
///
/// Any time after creation, a publisher can start to send messages using its
/// [Sink](futures::sink::Sink) implementation.
pub struct Publisher {
    shared: ProtectedShared,
    sink: Sender<DataGram>,
    generation: Generation,
    in_poll: Option<Pin<Box<(dyn Future<Output = Result<()>>)>>>,
}

impl Publisher {
    /// Create a new [Publisher] without a description
    ///
    /// The [Publisher] will have been registered with the provided [find_service::Client] and be
    /// ready to start sending messages.
    pub async fn from_description(
        desc: PublisherDesc,
        client: find_service::Client,
    ) -> Result<Self> {
        let subscriber_expiration_interval = desc.subscriber_expiration_interval;
        let shared = Arc::new(Mutex::new(PublisherShared::new(
            true,
            subscriber_expiration_interval,
        )));
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

        publisher_registration(desc, client, Arc::clone(&shared)).await?;

        Ok(Self {
            shared,
            sink,
            generation: 1,
            in_poll: None,
        })
    }

    fn flush(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        loop {
            if let Some(future) = self.in_poll.as_mut() {
                futures::ready!(future.as_mut().poll(cx))?;
                self.in_poll.take();
            } else {
                return Poll::Ready(Ok(()));
            }
        }
    }
}

impl Drop for Publisher {
    /// Ensure messages are cleaned up on drop
    fn drop(&mut self) {
        futures::executor::block_on(self.shared.lock()).is_active = false;
    }
}

impl<Buf> Sink<Buf> for Publisher
where
    Buf: bytes::Buf,
{
    type Error = Error;

    fn start_send(self: Pin<&mut Self>, mut item: Buf) -> Result<()> {
        let pin = self.get_mut();

        let timestamp = time::SystemTime::now();
        let generation = pin.generation;
        let shared = pin.shared.clone();
        let mut sink = pin.sink.clone();
        let msgs = Message::split_data_msgs(item.to_bytes(), generation)?;
        pin.in_poll.replace(Box::pin(async move {
            let dgrams = {
                let mut shared = shared.lock().await;
                shared.prune_stale_subscriptions(timestamp);
                Vec::from_iter(
                    msgs.into_iter()
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

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        self.get_mut().flush(cx)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        self.get_mut().flush(cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        let pin = self.get_mut();
        let ret = pin.flush(cx);
        futures::executor::block_on(pin.shared.lock()).is_active = false;
        ret
    }
}

#[cfg(test)]
mod tests {}
