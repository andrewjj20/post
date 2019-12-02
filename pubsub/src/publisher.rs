use super::find_service;
use super::framing;
use super::framing::{Acknowledgement, Message, MessageCodec, Request};
use super::{DataGram, Error, Generation, PublisherDesc, Result};
use futures::{
    future::{self, Future},
    Async, AsyncSink, Poll, Sink, StartSend, Stream,
};
use itertools::Itertools;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time;
use std::vec::Vec;
use tokio::net::UdpFramed;
use tokio::prelude::*;
use tokio::sync::mpsc::{self, Sender};
use tokio::timer;

#[derive(Debug)]
struct Subscriber {
    addr: SocketAddr,
    expiration: time::SystemTime,
}

struct PublisherShared {
    subscribers: HashMap<SocketAddr, Subscriber>,
    is_active: bool,
    subscriber_expiration: time::Duration,
}

impl PublisherShared {
    fn handle_subscription(&mut self, addr: SocketAddr) -> Option<DataGram> {
        let timestamp = time::SystemTime::now();
        let message =
            Message::Acknowledgement(Acknowledgement::Subscription(framing::Subscription {
                timeout_interval: self.subscriber_expiration,
            }));
        let expiration = timestamp + self.subscriber_expiration;
        match self.subscribers.get_mut(&addr) {
            Some(v) => {
                v.expiration = timestamp + self.subscriber_expiration;
                debug!("Subscription Renewed {:?}", v);
            }
            None => {
                let sub = Subscriber {
                    addr: addr,
                    expiration,
                };
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

pub struct Publisher {
    shared: ProtectedShared,
    sink: Sender<DataGram>,
    generation: Generation,
    current_send: Option<Vec<DataGram>>,
    in_poll: bool,
}

struct PublisherInternal<T> {
    protocol: T,
    shared: ProtectedShared,
}

impl<T> PublisherInternal<T> {
    fn new(protocol: T, shared: ProtectedShared) -> PublisherInternal<T> {
        PublisherInternal { protocol, shared }
    }

    fn is_active(&self) -> bool {
        self.shared.lock().unwrap().is_active
    }
}

impl<T> Stream for PublisherInternal<T>
where
    T: Stream<Item = DataGram, Error = Error>,
{
    type Item = DataGram;
    type Error = Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if self.is_active() {
            self.protocol.poll()
        } else {
            Ok(Async::Ready(None))
        }
    }
}

fn handle_publisher_backend(
    shared: Arc<Mutex<PublisherShared>>,
    incomming: DataGram,
) -> Option<DataGram> {
    debug!("Message received: {}", incomming.0);
    let ret = match incomming.0 {
        Message::Request(r) => match r {
            Request::Subscribe(_) => shared.lock().unwrap().handle_subscription(incomming.1),
            Request::Unsubscribe(_) => shared.lock().unwrap().handle_unsubscribe(&incomming.1),
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

fn publisher_registration(
    desc: PublisherDesc,
    find_uri: String,
    shared: Arc<Mutex<PublisherShared>>,
) {
    let reg_info = Arc::new((find_uri, desc));
    tokio::spawn(
        stream::unfold(std::time::Duration::new(0, 0), move |interval| {
            let fold_reg_info = Arc::clone(&reg_info);
            if shared.lock().unwrap().is_active {
                Some(
                    timer::Delay::new(std::time::Instant::now() + interval)
                        .map_err(log_err)
                        .and_then(move |_| {
                            let this_reg_info = Arc::clone(&fold_reg_info);
                            find_service::publisher_register(&this_reg_info.0, &this_reg_info.1)
                                .map_err(log_err)
                        })
                        .then(move |result| {
                            future::ok(match result {
                                Ok(resp) => ((), resp.response.expiration_interval / 2),
                                Err(_) => ((), interval),
                            })
                        }),
                )
            } else {
                None
            }
        })
        .for_each(|_| future::ok(())),
    );
}

impl Publisher {
    pub fn new(
        name: String,
        host_name: String,
        port: u16,
        subscriber_expiration: time::Duration,
        find_uri: String,
    ) -> Result<Publisher> {
        let desc = PublisherDesc {
            name,
            host_name,
            port,
            subscriber_expiration,
        };
        Publisher::from_description(desc, find_uri)
    }

    pub fn from_description(desc: PublisherDesc, find_uri: String) -> Result<Publisher> {
        let subscriber_expiration = desc.subscriber_expiration;
        let shared = Arc::new(Mutex::new(PublisherShared {
            subscribers: HashMap::new(),
            is_active: true,
            subscriber_expiration,
        }));
        let (udp_sink, udp_stream) =
            UdpFramed::new(desc.to_tokio_socket()?, MessageCodec {}).split();
        let (sink, stream) = mpsc::channel::<DataGram>(1);

        let reserved_shared = Arc::clone(&shared);
        tokio::spawn(
            PublisherInternal::new(udp_stream, Arc::clone(&shared))
                .filter_map(move |d| handle_publisher_backend(Arc::clone(&reserved_shared), d))
                .forward(sink.clone())
                .map_err(move |e| {
                    error!("Stream Error {}", e);
                })
                .map(|_| ()),
        );
        tokio::spawn(
            stream
                .map_err(|_| Error::Empty)
                .forward(udp_sink)
                .map(|_| ())
                .map_err(|e| {
                    error!("Sink Error {}", e);
                }),
        );

        publisher_registration(desc, find_uri, Arc::clone(&shared));

        Ok(Publisher {
            shared,
            sink,
            generation: 1,
            current_send: None,
            in_poll: false,
        })
    }

    fn reset_stream(&mut self) {
        self.in_poll = false;
        self.current_send = None;
    }

    fn do_send(&mut self) -> Poll<(), Error> {
        if self.in_poll {
            let poll_result = self.sink.poll_complete();
            if let Ok(a) = poll_result {
                if let Async::Ready(_) = a {
                    self.in_poll = false;
                }
            }
            Ok(poll_result?)
        } else {
            let current = self.current_send.as_mut().unwrap().pop().unwrap();
            debug!("sending packet ({},{})", &current.0, &current.1);
            match self.sink.start_send(current)? {
                AsyncSink::Ready => {
                    if self.current_send.as_mut().unwrap().is_empty() {
                        self.current_send = None;
                    }
                    Ok(Async::Ready(()))
                }
                AsyncSink::NotReady(c) => {
                    self.in_poll = true;
                    self.current_send.as_mut().unwrap().push(c);
                    Ok(Async::NotReady)
                }
            }
        }
    }

    fn do_send_loop(&mut self) -> Result<bool> {
        while self.current_send.is_some() && !self.in_poll {
            if let Async::NotReady = self.do_send()? {
                return Ok(false);
            }
        }
        Ok(!self.in_poll)
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.shared.lock().unwrap().is_active = false;
    }
}

impl Sink for Publisher {
    type SinkItem = Vec<u8>;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        if self.current_send.is_some() {
            return Ok(AsyncSink::NotReady(item));
        }

        let timestamp = time::SystemTime::now();
        {
            let mut shared = self.shared.lock().unwrap();
            shared.prune_stale_subscriptions(timestamp);
            let dgrams = Vec::from_iter(
                Message::split_data_msgs(item.as_slice(), self.generation)?
                    .into_iter()
                    .cartesian_product(shared.subscribers.values().map(|s| s.addr)),
            );
            if !dgrams.is_empty() {
                self.current_send = Some(dgrams);
            }
        }

        self.generation += 1;
        self.do_send_loop()?;
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        loop {
            try_ready!(match self.do_send() {
                Err(e) => {
                    self.reset_stream();
                    Err(e)
                }
                Ok(a) => Ok(a),
            });
            if self.current_send.is_none() && !self.in_poll {
                return Ok(Async::Ready(()));
            }
        }
    }
}

#[cfg(test)]
mod tests {}
