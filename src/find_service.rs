use super::{ConnectionInfo, PublisherDesc};
use futures::future::{self, Future, FutureResult};
use futures::stream;
use futures::Async;
use http::{self, StatusCode};
use hyper;
use hyper::rt::Stream;
use hyper::{header::HeaderValue, Body, Client, Method, Uri};
use serde;
use serde_json;
use std::clone::Clone;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use std::time;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BlankResponse {}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RegistrationResponse {
    pub expiration_interval: time::Duration,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ConnectionResponse {
    pub publisher: PublisherDesc,
    pub info: ConnectionInfo,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PubSubResponse<T>
where
    T: Clone,
{
    pub status: String,
    pub timestamp: time::SystemTime,
    pub response: T,
}

impl PubSubResponse<BlankResponse> {
    pub fn status_only(status: String) -> PubSubResponse<BlankResponse> {
        PubSubResponse {
            status,
            timestamp: time::SystemTime::now(),
            response: BlankResponse {},
        }
    }
}

impl PubSubResponse<RegistrationResponse> {
    pub fn registration(
        status: String,
        expiration_interval: time::Duration,
    ) -> PubSubResponse<RegistrationResponse> {
        PubSubResponse::new(
            status,
            RegistrationResponse {
                expiration_interval,
            },
        )
    }
}

pub type PubSubBlankResponse = PubSubResponse<BlankResponse>;

impl<T> PubSubResponse<T>
where
    T: Clone,
{
    pub fn new(status: String, response: T) -> PubSubResponse<T> {
        PubSubResponse {
            status,
            timestamp: time::SystemTime::now(),
            response,
        }
    }
}
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ServiceStatus {
    pub count: usize,
}

#[derive(Clone, Debug)]
pub struct PubSubServerError {
    status: StatusCode,
    response: PubSubBlankResponse,
}

impl PubSubServerError {
    fn new(status: StatusCode, response: PubSubBlankResponse) -> PubSubServerError {
        PubSubServerError { status, response }
    }
}

impl fmt::Display for PubSubServerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {}", self.status, self.response.status)
    }
}

impl Error for PubSubServerError {
    fn description(&self) -> &str {
        self.response.status.as_str()
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}

#[derive(Debug)]
pub enum ServerError {
    Success,
    HyperError(hyper::Error),
    SerdeError(serde_json::Error),
    UrlError(http::uri::InvalidUri),
    PubSubServerError(PubSubServerError),
    StringError(String),
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use crate::find_service::ServerError::*;
        match self {
            Success => write!(f, "Success"),
            HyperError(e) => write!(f, "hyper: {}", e),
            SerdeError(e) => write!(f, "json parser: {}", e),
            UrlError(e) => write!(f, "URL: {}", e),
            PubSubServerError(e) => write!(f, "URL: {}", e),
            StringError(e) => write!(f, "URL: {}", e),
        }
    }
}

impl Error for ServerError {
    fn description(&self) -> &str {
        "Server Error"
    }

    fn cause(&self) -> Option<&Error> {
        use crate::find_service::ServerError::*;
        match self {
            Success => None,
            HyperError(e) => Some(e),
            SerdeError(e) => Some(e),
            UrlError(e) => Some(e),
            PubSubServerError(e) => Some(e),
            StringError(_) => None,
        }
    }
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

impl From<serde_json::Error> for ServerError {
    fn from(hyper_err: serde_json::Error) -> ServerError {
        ServerError::SerdeError(hyper_err)
    }
}

impl From<PubSubServerError> for ServerError {
    fn from(hyper_err: PubSubServerError) -> ServerError {
        ServerError::PubSubServerError(hyper_err)
    }
}

enum RequestState {
    Creation(FutureResult<hyper::Request<hyper::Body>, ServerError>),
    RequestActive(hyper::client::ResponseFuture),
    ReadingBody(StatusCode, stream::Concat2<Body>),
}

pub struct RequestFuture<T> {
    state: RequestState,
    response: PhantomData<T>,
}

impl<T> RequestFuture<T> {
    fn new(req: FutureResult<hyper::Request<hyper::Body>, ServerError>) -> RequestFuture<T> {
        RequestFuture {
            state: RequestState::Creation(req),
            response: PhantomData,
        }
    }
}

impl<T> Future for RequestFuture<T>
where
    T: serde::de::DeserializeOwned + Clone,
{
    type Item = PubSubResponse<T>;
    type Error = ServerError;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        use crate::find_service::RequestState::{Creation, ReadingBody, RequestActive};
        loop {
            self.state = match self.state {
                Creation(ref mut req_future) => {
                    let req = try_ready!(req_future.poll());
                    debug!("request processed, {} {}", req.method(), req.uri());
                    RequestActive(Client::new().request(req))
                }
                RequestActive(ref mut res_future) => {
                    let res = try_ready!(res_future.poll());
                    debug!("Processing Response, code {}", res.status());
                    ReadingBody(res.status(), res.into_body().concat2())
                }
                ReadingBody(status, ref mut body) => {
                    let raw_body = try_ready!(body.poll());
                    let ret;
                    if status.is_success() {
                        let deserialized = serde_json::from_slice::<Self::Item>(&raw_body)?;
                        ret = Ok(Async::Ready(deserialized));
                    } else {
                        let deserialized =
                            serde_json::from_slice::<PubSubBlankResponse>(&raw_body)?;
                        ret = Err(ServerError::PubSubServerError(PubSubServerError::new(
                            status,
                            deserialized,
                        )));
                    }
                    return ret;
                }
            };
        }
    }
}

pub fn server_status(base_uri: &str) -> RequestFuture<ServiceStatus> {
    let handoff = match format!("{}/status", base_uri).parse::<Uri>() {
        Err(e) => Err(ServerError::from(e)),
        Ok(url) => {
            let mut req = hyper::Request::new(Body::empty());
            *req.method_mut() = Method::GET;
            *req.uri_mut() = url;
            Ok(req)
        }
    };

    RequestFuture::<ServiceStatus>::new(future::result(handoff))
}

pub fn get_descriptors_for_name(base_uri: &str, name: &str) -> RequestFuture<ConnectionResponse> {
    let uri_as_str = format!("{}/publishers/{}", base_uri, name);
    RequestFuture::<ConnectionResponse>::new(future::result(match uri_as_str.parse::<Uri>() {
        Err(e) => Err(ServerError::from(e)),
        Ok(url) => {
            let mut req = hyper::Request::new(Body::empty());
            *req.method_mut() = Method::GET;
            *req.uri_mut() = url;
            Ok(req)
        }
    }))
}

pub fn get_descriptors(base_uri: &str) -> RequestFuture<HashMap<String, ConnectionResponse>> {
    let uri_as_str = format!("{}/publishers/", base_uri);
    RequestFuture::new(future::result(match uri_as_str.parse::<Uri>() {
        Err(e) => Err(ServerError::from(e)),
        Ok(url) => {
            let mut req = hyper::Request::new(Body::empty());
            *req.method_mut() = Method::GET;
            *req.uri_mut() = url;
            Ok(req)
        }
    }))
}

pub fn publisher_register(
    base_uri: &str,
    publister: &PublisherDesc,
) -> RequestFuture<RegistrationResponse> {
    let json = match serde_json::to_vec(&publister) {
        Err(e) => {
            return RequestFuture::new(future::result(Err(ServerError::from(e))));
        }
        Ok(serialized) => serialized,
    };
    let handoff = match format!("{}/publishers/", base_uri).parse::<Uri>() {
        Err(e) => Err(ServerError::from(e)),
        Ok(url) => {
            let mut req = hyper::Request::new(Body::from(json));
            *req.method_mut() = Method::POST;
            *req.uri_mut() = url;
            req.headers_mut().insert(
                "content-type",
                HeaderValue::from_str("application/json").unwrap(),
            );
            Ok(req)
        }
    };

    RequestFuture::new(future::result(handoff))
}
