use super::find_service::{self, PubSubResponse, RegistrationResponse};
use super::framing::{Message, MessageCodec, Request};
use super::{DataGram, Error, Generation, PublisherDesc, Result};
use futures::{
    future::{self, Future},
    sync::mpsc::{self, Sender},
    Async, AsyncSink, Poll, Sink, StartSend, Stream,
};
use itertools::Itertools;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::vec::Vec;
use tokio::{
    net::UdpFramed,
};

struct Subscriber {
    _addr: SocketAddr,
}

struct PublisherShared {
    subscribers: HashMap<SocketAddr, Subscriber>,
    is_active: bool,
}

impl PublisherShared {
    fn handle_subscription(&mut self, addr: SocketAddr) {
        let sub = Subscriber {
            _addr: addr.clone(),
        };
        self.subscribers.insert(addr, sub);
    }
    fn handle_unsubscribe(&mut self, addr: &SocketAddr) {
        self.subscribers.remove(addr);
    }
}

type ProtectedShared = Arc<Mutex<PublisherShared>>;

pub struct Publisher {
    shared: Arc<Mutex<PublisherShared>>,
    sink: Sender<DataGram>,
    generation: Generation,
    current_send: Option<Vec<DataGram>>,
    in_poll: bool,
}

struct PublisherInternal<T> {
    protocol: T,
    shared: Arc<Mutex<PublisherShared>>,
}

impl<T> PublisherInternal<T> {
    fn new(protocol: T, shared: Arc<Mutex<PublisherShared>>) -> PublisherInternal<T> {
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
    match incomming.0 {
        Message::Request(r) => match r {
            Request::Subscribe(_) => {
                info!("Subscripton {}", &incomming.1);
                shared.lock().unwrap().handle_subscription(incomming.1);
            }
            Request::Unsubscribe(_) => {
                shared.lock().unwrap().handle_unsubscribe(&incomming.1);
            }
        },
        Message::Data(_) => error!("Data Message sent to publisher from {}", incomming.1),
        _ => error!(
            "Unhandled Message received from {} :{}",
            incomming.1, incomming.0
        ),
    };
    None
}

fn log_err<T>(e: T)
where
    T: std::fmt::Display,
{
    error!("Registration Error {}", e);
}

fn initial_publisher_registration(
    desc: PublisherDesc,
    find_uri: String,
    shared: ProtectedShared,
) -> impl Future<Item = PubSubResponse<RegistrationResponse>, Error = ()> {
    let mut initial_future: Option<find_service::RequestFuture<RegistrationResponse>> = None;
    future::poll_fn(move || loop {
        match initial_future {
            Some(ref mut f) => match f.poll() {
                Ok(a) => return Ok(a),
                Err(e) => {
                    if shared.lock().unwrap().is_active {
                        initial_future = None;
                    } else {
                        return Err(e);
                    }
                }
            },
            None => initial_future = Some(find_service::publisher_register(&find_uri, &desc)),
        }
    })
    .map_err(log_err)
}

fn publisher_registration(
    desc: PublisherDesc,
    find_uri: String,
    shared: Arc<Mutex<PublisherShared>>,
) {
    tokio::spawn(
        initial_publisher_registration(desc.clone(), find_uri.clone(), shared.clone()).and_then(
            move |resp| {
                let interval = resp.response.expiration_interval / 2;
                info!("Registration interval {:?}", interval);
                tokio::timer::Interval::new(std::time::Instant::now() + interval, interval)
                    .map_err(log_err)
                    .for_each(move |_| {
                        find_service::publisher_register(&find_uri, &desc)
                            .map_err(log_err)
                            .map(|_| ())
                    })
            },
        ),
    );
}

impl Publisher {
    pub fn new<'a>(
        name: String,
        host_name: String,
        port: u16,
        find_uri: String,
    ) -> Result<Publisher> {
        let desc = PublisherDesc {
            name,
            host_name,
            port,
        };
        Publisher::from_description(desc, find_uri)
    }

    pub fn from_description(desc: PublisherDesc, find_uri: String) -> Result<Publisher> {
        let shared = Arc::new(Mutex::new(PublisherShared {
            subscribers: HashMap::new(),
            is_active: true,
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

        {
            let shared = self.shared.lock().unwrap();
            self.current_send = Some(Vec::from_iter(
                Message::split_data_msgs(item.as_slice(), self.generation)?
                    .into_iter()
                    .cartesian_product(shared.subscribers.keys().cloned()),
            ));
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
