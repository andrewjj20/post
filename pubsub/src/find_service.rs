pub mod proto;

use super::PublisherDesc;
use convert::{TryFrom, TryInto};
use proto::find_me_client::FindMeClient;
use std::fmt::Write;
use std::{convert, error, fmt, result, time};
use tonic::{Request, Status};

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

impl error::Error for MissingFieldError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

impl TryInto<Status> for MissingFieldError {
    type Error = fmt::Error;
    fn try_into(self) -> Result<Status, Self::Error> {
        let mut text = String::new();
        write!(&mut text, "{}", self)?;
        Ok(Status::invalid_argument(text))
    }
}

pub async fn server_status(
    base_uri: &str,
) -> Result<proto::StatusResponse, Box<dyn std::error::Error>> {
    let mut client = FindMeClient::connect(base_uri.to_string()).await?;

    let request = Request::new(proto::StatusRequest {});

    let response = client.server_status(request).await?;
    Ok(response.into_inner())
}

pub async fn get_descriptors_for_name(
    base_uri: String,
    name: String,
) -> Result<proto::SearchResponse, Box<dyn std::error::Error>> {
    let mut client = FindMeClient::connect(base_uri).await?;

    let request = Request::new(proto::SearchRequest {
        name_regex: name.to_string(),
    });

    let response = client.get_publishers(request).await?;
    Ok(response.into_inner())
}

pub async fn get_descriptors(
    base_uri: &str,
) -> Result<proto::SearchResponse, Box<dyn std::error::Error>> {
    let mut client = FindMeClient::connect(base_uri.to_string()).await?;

    let request = Request::new(proto::SearchRequest {
        name_regex: "*".to_string(),
    });

    let response = client.get_publishers(request).await?;
    Ok(response.into_inner())
}

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

pub async fn publisher_register(
    base_uri: String,
    publisher: PublisherDesc,
) -> Result<RegistrationResponse, Box<dyn std::error::Error>> {
    let mut client = FindMeClient::connect(base_uri).await?;

    let request = Request::new(proto::RegistrationRequest {
        desc: Some(proto::PublisherDesc::from(publisher)),
    });

    let response = client.publisher_register(request).await?;
    Ok(RegistrationResponse::try_from(response.into_inner())?)
}
