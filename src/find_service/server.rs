use super::{proto, proto::find_me_server::FindMe, MissingFieldError};
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::io;
use std::result::Result;
use std::time;
use time::Duration;
use time::SystemTime;
use tonic::{Request, Response, Status};

fn convert_system_time_error(time_error: time::SystemTimeError) -> io::Error {
    io::Error::new(io::ErrorKind::Other, time_error)
}

#[derive(Debug, Clone)]
pub struct PublisherNotFoundError;

impl fmt::Display for PublisherNotFoundError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid first item to double")
    }
}

pub trait PublisherStore: Send + Sync + Clone + 'static {
    fn insert_publisher(&self, publisher_name: &str, registration: proto::Registration);
    fn remove_publisher(&self, publisher_name: &str) -> Result<(), PublisherNotFoundError>;
    fn remove_publishers(&self, publisher_names: &[String]) {
        for publisher in publisher_names {
            let _ = self.remove_publisher(publisher);
        }
    }
    fn find_publisher(&self, publisher_name: &str) -> Option<proto::Registration>;
    fn get_publishers(&self) -> Vec<(String, proto::Registration)>;
    fn find_publishers(&self, search_str: &str) -> Vec<(String, proto::Registration)>;
}

#[derive(Default, Clone)]
pub struct MeetupServer<T: PublisherStore + Clone> {
    // publisher_store: PublisherStoreMap,
    publisher_timeout: Duration,
    publisher_store: T,
}

#[derive(Default)]
pub struct MeetupServerOptions<T: PublisherStore> {
    pub publisher_timeout: Duration,
    pub publisher_store: T,
}

impl<T: PublisherStore + Clone + 'static> MeetupServer<T> {
    pub fn new(options: MeetupServerOptions<T>) -> MeetupServer<T> {
        MeetupServer {
            publisher_timeout: options.publisher_timeout,
            publisher_store: options.publisher_store,
        }
    }
}

#[tonic::async_trait]
impl<T: PublisherStore> FindMe for MeetupServer<T> {
    async fn server_status(
        &self,
        _tonic_request: Request<proto::StatusRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let reply = proto::StatusResponse {
            count: self.publisher_store.get_publishers().len() as u64,
        };
        Ok(Response::new(reply))
    }
    async fn publisher_register(
        &self,
        tonic_request: Request<proto::RegistrationRequest>,
    ) -> Result<Response<proto::RegistrationResponse>, Status> {
        let request = tonic_request.into_inner();
        let last_report =
            Some(proto::Time::try_from(SystemTime::now()).map_err(convert_system_time_error)?);
        let expiration = Some(
            proto::Time::try_from(SystemTime::now() + self.publisher_timeout)
                .map_err(convert_system_time_error)?,
        );
        let name;
        let registration = proto::Registration {
            publisher: match request.desc {
                Some(desc) => {
                    name = desc.name.clone();
                    Some(desc)
                }
                None => {
                    return Err(MissingFieldError::new("Registration", "desc")
                        .try_into()
                        .unwrap())
                }
            },
            info: Some(proto::ConnectionInfo {
                last_report,
                expiration,
            }),
        };
        {
            self.publisher_store.insert_publisher(&name, registration);
        }

        let reply = proto::RegistrationResponse {
            expiration_interval: Some(self.publisher_timeout.into()),
        };
        Ok(Response::new(reply))
    }

    async fn get_publishers(
        &self,
        tonic_request: Request<proto::SearchRequest>,
    ) -> Result<Response<proto::SearchResponse>, Status> {
        let request = tonic_request.into_inner();

        let publishers = self.publisher_store.find_publishers(&request.name_regex);
        let list = if let Some((_name, val)) = publishers.get(0) {
            vec![val.clone()]
        } else {
            vec![]
        };

        let reply = proto::SearchResponse { list };
        Ok(Response::new(reply))
    }
}
