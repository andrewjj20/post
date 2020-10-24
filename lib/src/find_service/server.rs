use super::{proto, proto::find_me_server::FindMe, MissingFieldError};
use futures::{future, stream::StreamExt};
use log::*;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::io;
use std::result::Result;
use std::sync::{Arc, RwLock};
use std::time;
use time::Duration;
use time::SystemTime;
use tonic::{Request, Response, Status};

fn convert_system_time_error(time_error: time::SystemTimeError) -> io::Error {
    io::Error::new(io::ErrorKind::Other, time_error)
}

type PublisherStore = Arc<RwLock<HashMap<String, proto::Registration>>>;

#[derive(Default, Clone)]
pub struct MeetupServer {
    publisher_store: PublisherStore,
    publisher_timeout: Duration,
    publisher_scan_interval: Duration,
}

pub struct MeetupServerOptions {
    pub publisher_timeout: Duration,
    pub publisher_scan_interval: Duration,
}

impl MeetupServer {
    pub fn new(options: MeetupServerOptions) -> MeetupServer {
        MeetupServer {
            publisher_store: Arc::new(RwLock::new(HashMap::new())),
            publisher_timeout: options.publisher_timeout,
            publisher_scan_interval: options.publisher_scan_interval,
        }
    }

    pub fn start_remove_process(&self) {
        self.remove_expired_publishers();
    }

    fn remove_expired_publishers(&self) {
        let pub_store = self.publisher_store.clone();
        let scan_interval = self.publisher_scan_interval;

        let _tokio_task = tokio::spawn(tokio::time::interval(scan_interval).for_each(move |_| {
            let now = time::SystemTime::now();
            pub_store.write().unwrap().retain(|_k, v| {
                if let Some(info) = &v.info {
                    if let Some(expiration) = &info.expiration {
                        match expiration.try_into() {
                            Result::<time::SystemTime, _>::Ok(proto_time) => proto_time > now,
                            Err(error) => {
                                error!("Removing descriptor, Invalid time: {}", error);
                                false
                            }
                        }
                    } else {
                        error!("Removing descriptor, No Expiration");
                        false
                    }
                } else {
                    error!("Removing descriptor, No ConnectionInfo");
                    false
                }
            });
            future::ready(())
        }));
    }
}

#[tonic::async_trait]
impl FindMe for MeetupServer {
    async fn server_status(
        &self,
        _tonic_request: Request<proto::StatusRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let locked = self.publisher_store.write().unwrap();

        let reply = proto::StatusResponse {
            count: locked.len() as u64,
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
            let mut locked = self.publisher_store.write().unwrap();
            locked.insert(name, registration);
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
        let locked = self.publisher_store.read().unwrap();

        let list = if let Some(val) = locked.get(&request.name_regex) {
            vec![val.clone()]
        } else {
            vec![]
        };

        let reply = proto::SearchResponse { list };
        Ok(Response::new(reply))
    }
}
