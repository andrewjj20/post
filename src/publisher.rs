use super::find_service;
use super::framing::{self, Message, Request};
use super::{ConnectionInfo, Error, PublisherDesc};
use futures::{
    future::Future,
    stream::Stream,
    sync::mpsc::{self, Receiver, Sender},
    Async, Poll,
};
use std::collections::HashSet;
use std::mem::size_of;
use std::net::SocketAddr;
use std::slice::Iter;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::vec::Vec;
use tokio::io;
use tokio::net::UdpSocket;

pub struct PublisherSetup {
    desc: PublisherDesc,
    request: find_service::RequestFuture<find_service::BlankResponse>,
}

impl PublisherSetup {
    pub fn new(
        desc: PublisherDesc,
        request: find_service::RequestFuture<find_service::BlankResponse>,
    ) -> PublisherSetup {
        PublisherSetup { desc, request }
    }
}

impl Future for PublisherSetup {
    type Item = Publisher;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        try_ready!(self.request.poll());
        let socket = self.desc.to_tokio_socket()?;
        Ok(Async::Ready(Publisher::from_socket(
            self.desc.clone(),
            socket,
        )))
    }
}

struct PublisherShared {
    desc: PublisherDesc,
}

pub struct Publisher {
    shared: Arc<PublisherShared>,
    publish_channel: Sender<Vec<u8>>,
}

enum SendStatus<'a> {
    Done,
    InProgresss(SocketAddr, Iter<'a, Vec<u8>>),
}

struct SendInfo<'a> {
    frames: Vec<Vec<u8>>,
    frame: Iter<'a, Vec<u8>>,
    subscribers: Iter<'a, Vec<u8>>,
}

trait DataGram {
    fn poll_recv_from(&mut self, buf: &mut [u8]) -> io::Result<Async<(usize, SocketAddr)>>;
}

impl DataGram for UdpSocket {
    fn poll_recv_from(&mut self, buf: &mut [u8]) -> io::Result<Async<(usize, SocketAddr)>> {
        self.poll_recv_from(buf)
    }
}

struct PublisherInternal<'a, S>
where
    S: DataGram,
{
    shared: Arc<PublisherShared>,
    publish_channel: Receiver<Vec<u8>>,
    socket: S,
    generation: usize,
    subscribers: HashSet<SocketAddr>,
    publish: Option<SendInfo<'a>>,
}

impl<'a, S> PublisherInternal<'a, S>
where
    S: DataGram,
{
    fn new(
        shared: Arc<PublisherShared>,
        receiver: Receiver<Vec<u8>>,
        socket: S,
    ) -> PublisherInternal<'a, S> {
        PublisherInternal {
            shared: shared,
            publish_channel: receiver,
            socket,
            generation: 0,
            subscribers: HashSet::new(),
            publish: None,
        }
    }

    fn add_subscriber(&mut self, addr: SocketAddr) {
        info!("{} subscribed", addr);
        self.subscribers.insert(addr);
    }

    fn remove_subscriber(&mut self, addr: SocketAddr) {
        info!("{} unsubscribed", addr);
        self.subscribers.remove(&addr);
    }

    fn handle_message(&mut self) -> Result<Async<()>, ()> {
        let size = size_of::<Message>();
        let mut buf: Vec<u8> = Vec::new();
        buf.resize(size, 0);
        match self.socket.poll_recv_from(buf.as_mut_slice()) {
            Err(e) => {
                error!("Receive Error {}", e);
                Err(())
            }
            Ok(r) => match r {
                Async::NotReady => Ok(Async::NotReady),
                Async::Ready(r) => {
                    let (size, addr) = r;
                    buf.truncate(size);
                    match Message::deserialize(&buf) {
                        Err(e) => error!("Deserializing ({}), {}", addr, e),
                        Ok(m) => match m {
                            Message::Data(_) => warn!("Publisher received Data message"),
                            Message::Request(r) => {
                                match r {
                                    Request::Subscribe(_) => self.add_subscriber(addr),
                                    Request::Unsubscribe(_) => self.remove_subscriber(addr),
                                };
                            }
                        },
                    };
                    Ok(Async::Ready(()))
                }
            },
        }
    }
}

impl<'a, S> Future for PublisherInternal<'a, S>
where
    S: DataGram,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let mut processed_one = false;
            if let Async::Ready(_) = self.handle_message()? {
                processed_one = true;
            }

            if !processed_one {
                return Ok(Async::NotReady);
            }
        }
    }
}

impl Publisher {
    pub fn new(name: String, host_name: String, port: u16, find_uri: &str) -> PublisherSetup {
        let desc = PublisherDesc {
            name,
            host_name,
            port,
        };
        let req = find_service::publisher_register(find_uri, &desc);
        PublisherSetup::new(desc, req)
    }

    fn from_socket(desc: PublisherDesc, socket: UdpSocket) -> Publisher {
        let shared = Arc::new(PublisherShared { desc });

        let (sender, receiver) =
            mpsc::channel::<Vec<u8>>(framing::MAX_DATA_SIZE + size_of::<Vec<u8>>());

        tokio::spawn(PublisherInternal::new(
            Arc::clone(&shared),
            receiver,
            socket,
        ));

        Publisher {
            shared,
            publish_channel: sender,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::framing::BaseMsg;
    use super::*;
    use std::net::ToSocketAddrs;
    use std::vec::Drain;
    type DGramRecv<'a> = Drain<'a, (&'a [u8], io::Result<Async<SocketAddr>>)>;
    struct MockDataGramSimple<'a> {
        recv_queue: DGramRecv<'a>,
    }

    impl<'a> DataGram for MockDataGramSimple<'a> {
        fn poll_recv_from(&mut self, buf: &mut [u8]) -> io::Result<Async<(usize, SocketAddr)>> {
            let next = self.recv_queue.next().expect("Asked for too many packets");
            match next.1? {
                Async::NotReady => Ok(Async::NotReady),
                Async::Ready(addr) => {
                    let len = std::cmp::min(buf.len(), next.0.len());
                    //I may just panic here in the future
                    buf[..len].copy_from_slice(&next.0[..len]);
                    Ok(Async::Ready((next.0.len(), addr)))
                }
            }
        }
    }

    fn msg(msg: Message) -> (Message, Vec<u8>) {
        let serialized = msg.serialize().unwrap();
        (msg, serialized)
    }

    fn setup(
        desc: PublisherDesc,
        recv: DGramRecv<'a>,
    ) -> (
        Arc<PublisherShared>,
        PublisherInternal<MockDataGramSimple>,
        Sender<Vec<u8>>,
    ) {
        let (sender, receiver) =
            mpsc::channel::<Vec<u8>>(framing::MAX_DATA_SIZE + size_of::<Vec<u8>>());

        let shared = Arc::new(PublisherShared { desc });
        let mock_dgram = MockDataGramSimple { recv_queue: recv };

        let mut publisher_back = PublisherInternal::new(Arc::clone(&shared), receiver, mock_dgram);
        (shared, publisher_back, sender)
    }

    #[test]
    fn subscription() {
        let desc = PublisherDesc {
            name: "test".to_string(),
            host_name: "127.0.0.1".to_string(),
            port: 42,
        };
        let addr = desc
            .to_socket_addrs()
            .expect("Address translation problem")
            .next()
            .expect("Address translation problem");
        let subscribe = msg(Message::Request(Request::Subscribe(BaseMsg {})));
        let unsubscribe = msg(Message::Request(Request::Unsubscribe(BaseMsg {})));
        let mut recv_vec = vec![
            (subscribe.1.as_slice(), Ok(Async::Ready(addr.clone()))),
            (&[], Ok(Async::NotReady)),
            (unsubscribe.1.as_slice(), Ok(Async::Ready(addr.clone()))),
            (&[], Ok(Async::NotReady)),
        ];

        let (_shared, mut publisher_back, _sender) = setup(desc, recv_vec.drain(..));

        assert!(publisher_back.subscribers.is_empty());
        publisher_back
            .poll()
            .expect("The publisher backend should not error here");

        assert!(publisher_back.subscribers.contains(&addr));
        publisher_back
            .poll()
            .expect("The publisher backend should not error here");
        assert!(publisher_back.subscribers.is_empty());
    }
}
