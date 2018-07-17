use super::{ConnectionInfo, PublisherDesc};
use futures::future::{self, Future};
use futures::prelude::*;
use futures::sync::oneshot::{self, Receiver, Sender};
use futures::Async;
use http;
use hyper;
use hyper::rt::Stream;
use hyper::{client::HttpConnector, Body, Client, Uri};
pub use io::Result;
use serde_json;
use std::result;
use std::time;
use tokio;

#[derive(Serialize, Deserialize, Debug)]
pub struct BlankResponse {}

#[derive(Serialize, Deserialize, Debug)]
pub struct ConnectionResponse {
    pub publisher: PublisherDesc,
    pub info: ConnectionInfo,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PubSubResponse<T> {
    pub status: String,
    pub timestamp: time::SystemTime,
    pub response: T,
}

impl PubSubResponse<BlankResponse> {
    pub fn status_only(status: String) -> PubSubResponse<BlankResponse> {
        PubSubResponse {
            status: status,
            timestamp: time::SystemTime::now(),
            response: BlankResponse {},
        }
    }
}

impl<T> PubSubResponse<T> {
    pub fn new(status: String, response: T) -> PubSubResponse<T> {
        PubSubResponse {
            status: status,
            timestamp: time::SystemTime::now(),
            response: response,
        }
    }
}
#[derive(Serialize, Deserialize, Debug)]
pub struct ServiceStatus {
    pub count: usize,
}

type ServiceResponse = PubSubResponse<ServiceStatus>;

pub enum ServerError {
    Success,
    HyperError(hyper::Error),
    SerdeError(serde_json::Error),
    UrlError(http::uri::InvalidUri),
    StringError(String),
}

impl From<http::uri::InvalidUri> for ServerError {
    fn from(uri_err: http::uri::InvalidUri) -> ServerError {
        ServerError::UrlError(uri_err)
    }
}

impl From<hyper::Error> for ServerError {
    fn from(hyper_err: hyper::Error) -> ServerError {
        ServerError::HyperError(hyper_err)
    }
}

type FindClient = Client<HttpConnector, Body>;

pub struct ResponseFuture<T> {
    receiver: Receiver<result::Result<PubSubResponse<T>, ServerError>>,
    error: ServerError,
}

impl<T> ResponseFuture<T> {
    fn new() -> (
        ResponseFuture<T>,
        Sender<result::Result<PubSubResponse<T>, ServerError>>,
    ) {
        let (tx, rx) = oneshot::channel();
        (
            ResponseFuture {
                receiver: rx,
                error: ServerError::Success,
            },
            tx,
        )
    }
}

impl<'a, T> Future for ResponseFuture<T> {
    type Item = PubSubResponse<T>;
    type Error = ServerError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.receiver.poll() {
            Err(_) => {
                return Err(ServerError::StringError(String::from(
                    "Connection abruptly closed",
                )))
            }
            Ok(a) => match a {
                Async::NotReady => Ok(Async::NotReady),
                Async::Ready(t) => match t {
                    Err(e) => Err(e),
                    Ok(r) => Ok(Async::Ready(r)),
                },
            },
        }
    }
}

pub fn server_status(base_uri: Uri) -> ResponseFuture<ServiceStatus> {
    let (ret, tx) = ResponseFuture::new();
    tokio::spawn(
        future::result(format!("{}/status", base_uri).parse::<Uri>())
            .map_err(|err| ServerError::UrlError(err))
            .and_then(|uri| {
                let client = Client::new();

                client
                    .get(uri)
                    .and_then(|res| {
                        res.into_body()
                            .concat2()
                    })
                    .map_err(|err| ServerError::HyperError(err))
            })
            .and_then(|body| {
                future::result(serde_json::from_slice::<ServiceResponse>(&body))
                    .map_err(|err| ServerError::SerdeError(err))
            })
            .then(|res| {
                if let Err(_) = tx.send(res) {
                    info!("Status request unused");
                }
                future::ok::<(), ()>(())
            }),
    );
    ret
}
