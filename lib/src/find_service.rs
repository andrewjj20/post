/// The find service protobuf back end. Use this when writing a Meetme service.
pub mod proto;

use super::PublisherDesc;
use convert::{TryFrom, TryInto};
use proto::find_me_client::FindMeClient;
use std::fmt::Write;
use std::{convert, error, fmt, result, time};
use tonic::{transport, Request, Status};

/// Error representing a missing optional in a registration response
#[derive(Debug)]
pub struct MissingFieldError {
    message_type: &'static str,
    field: &'static str,
}

impl MissingFieldError {
    pub fn new(message_type: &'static str, field: &'static str) -> MissingFieldError {
        MissingFieldError {
            message_type,
            field,
        }
    }
}

impl fmt::Display for MissingFieldError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Expected field '{}' in message '{}' was not found",
            self.field, self.message_type
        )
    }
}

impl error::Error for MissingFieldError {}

impl TryInto<Status> for MissingFieldError {
    type Error = fmt::Error;
    fn try_into(self) -> std::result::Result<Status, Self::Error> {
        let mut text = String::new();
        write!(&mut text, "{}", self)?;
        Ok(Status::invalid_argument(text))
    }
}

/// Wrapper for [proto::RegistrationResponse]
///
/// The fields here are not optional. This represents a complete response.
pub struct RegistrationResponse {
    pub expiration_interval: time::Duration,
}

impl convert::TryFrom<proto::RegistrationResponse> for RegistrationResponse {
    type Error = MissingFieldError;
    fn try_from(resp: proto::RegistrationResponse) -> result::Result<Self, Self::Error> {
        let expiration_interval = match resp.expiration_interval {
            None => {
                return Err(MissingFieldError::new(
                    "RegistrationResponse",
                    "expiration_interval",
                ))
            }
            Some(interval) => interval.into(),
        };
        Ok(RegistrationResponse {
            expiration_interval,
        })
    }
}

/// Builder for clients
#[derive(Clone, Debug)]
pub struct ClientBuilder {
    endpoint: transport::Endpoint,
    connect_timeout: Option<time::Duration>,
}

use tonic::transport::Uri;

use std::result::Result;

/// Generic error when connecting to a Meetme service.
#[derive(Debug)]
pub struct ClientConnectError {
    inner: tonic::transport::Error,
}

impl std::convert::From<tonic::transport::Error> for ClientConnectError {
    fn from(inner: tonic::transport::Error) -> Self {
        Self { inner }
    }
}

impl std::fmt::Display for ClientConnectError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "Error Connecting to find service: {}", self.inner)
    }
}

impl std::error::Error for ClientConnectError {}

impl ClientBuilder {
    /// Create a client from the url specified in s.
    pub fn from_url<T>(s: T) -> Result<ClientBuilder, T::Error>
    where
        T: TryInto<Uri>,
    {
        let uri: Uri = s.try_into()?;
        Ok(ClientBuilder {
            endpoint: uri.into(),
            connect_timeout: None,
        })
    }

    /// Set the timeout on connecting to the Meetme service
    pub fn set_connect_timeout(mut self, timeout: time::Duration) -> Self {
        self.connect_timeout.replace(timeout);
        self
    }

    /// Connect to the specified Meetme service.
    ///
    /// This async function completes when the connection is has been made or a fatal error has
    /// occurred.
    pub async fn connect(self) -> Result<Client, ClientConnectError> {
        let mut http = hyper::client::connect::HttpConnector::new();
        http.set_connect_timeout(self.connect_timeout);
        let channel = self.endpoint.connect_with_connector(http).await?;

        Ok(Client {
            inner: FindMeClient::new(channel),
        })
    }
    /// Connect to the Meetme service asynchronously
    pub fn connect_lazy(self) -> Result<Client, ClientConnectError> {
        Ok(Client {
            inner: FindMeClient::new(self.endpoint.connect_lazy()?),
        })
    }
}

/// Higher level wrapper for errors occurring when communicating with a Meetme service.
#[derive(Debug)]
pub enum ClientError {
    ProtocolError(Status),
    MissingField(MissingFieldError),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::ProtocolError(status) => {
                write!(fmt, "Error contacting server: {}", status)
            }
            ClientError::MissingField(error) => write!(fmt, "{}", error),
        }
    }
}

impl std::error::Error for ClientError {}

impl std::convert::From<tonic::Status> for ClientError {
    fn from(status: tonic::Status) -> Self {
        Self::ProtocolError(status)
    }
}

impl std::convert::From<MissingFieldError> for ClientError {
    fn from(error: MissingFieldError) -> Self {
        ClientError::MissingField(error)
    }
}

/// Meetme service client. This client uses [proto] to implement a more convenient front end.
#[derive(Clone, Debug)]
pub struct Client {
    inner: FindMeClient<transport::Channel>,
}

impl Client {
    /// Create a client using [ClientBuilder].
    pub fn from_url<T>(s: T) -> Result<ClientBuilder, T::Error>
    where
        T: TryInto<Uri>,
        T::Error: std::error::Error + Send + Sync + 'static,
    {
        ClientBuilder::from_url(s)
    }

    /// Retrieve status information from the connected Meetme service.
    pub async fn server_status(&mut self) -> Result<proto::StatusResponse, ClientError> {
        let request = Request::new(proto::StatusRequest {});

        let response = self.inner.server_status(request).await?;
        Ok(response.into_inner())
    }

    /// Retrieve publisher descriptors matching a name from the connected Meetme service.
    pub async fn get_descriptors_for_name(
        &mut self,
        name: String,
    ) -> Result<proto::SearchResponse, ClientError> {
        let request = Request::new(proto::SearchRequest {
            name_regex: name.to_string(),
        });

        let response = self.inner.get_publishers(request).await?;
        Ok(response.into_inner())
    }

    /// Retrieve all publisher descriptors from the connected Meetme service.
    pub async fn get_descriptors(&mut self) -> Result<proto::SearchResponse, ClientError> {
        let request = Request::new(proto::SearchRequest {
            name_regex: "*".to_string(),
        });

        let response = self.inner.get_publishers(request).await?;
        Ok(response.into_inner())
    }

    /// Register a publisher with the connected Meetme service.
    pub async fn publisher_register(
        &mut self,
        publisher: PublisherDesc,
    ) -> Result<RegistrationResponse, ClientError> {
        let request = Request::new(proto::RegistrationRequest {
            desc: Some(proto::PublisherDesc::from(publisher)),
        });

        let response = self.inner.publisher_register(request).await?;
        Ok(RegistrationResponse::try_from(response.into_inner())?)
    }
}
