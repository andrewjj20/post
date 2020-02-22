extern crate tokio;
use clap::{crate_authors, App as ClApp, Arg};
use futures::{future, stream::StreamExt};
use log::*;
use pubsub::{
    find_service,
    find_service::{
        proto,
        proto::find_me_server::{FindMe, FindMeServer},
    },
};
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::io;
use std::net::ToSocketAddrs;
use std::result::Result;
use std::sync::{Arc, RwLock};
use std::time;
use time::{Duration, SystemTime};
use tonic::{transport::Server, Request, Response, Status};

fn convert_system_time_error(time_error: time::SystemTimeError) -> io::Error {
    io::Error::new(io::ErrorKind::Other, time_error)
}

#[derive(Default)]
struct ServerState {
    publishers: HashMap<String, proto::Registration>,
}

impl ServerState {
    pub fn new() -> ServerState {
        ServerState {
            publishers: HashMap::new(),
        }
    }
}

type Inner = Arc<RwLock<ServerState>>;

#[derive(Default, Clone)]
struct ProtectedState {
    inner: Inner,
    publisher_timeout: time::Duration,
}

#[tonic::async_trait]
impl FindMe for ProtectedState {
    async fn server_status(
        &self,
        _tonic_request: Request<proto::StatusRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let locked = self.inner.write().unwrap();

        let reply = proto::StatusResponse {
            count: locked.publishers.len() as u64,
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
                    return Err(find_service::MissingFieldError::new("Registration", "desc")
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
            let mut locked = self.inner.write().unwrap();
            locked.publishers.insert(name, registration);
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
        let locked = self.inner.read().unwrap();

        let list = if let Some(val) = locked.publishers.get(&request.name_regex) {
            vec![val.clone()]
        } else {
            unimplemented!()
        };

        let reply = proto::SearchResponse { list };
        Ok(Response::new(reply))
    }
}

fn remove_expired_publishers(s: Inner, i: time::Duration) {
    tokio::spawn(tokio::time::interval(i).for_each(move |_| {
        let now = time::SystemTime::now();
        s.write().unwrap().publishers.retain(|_k, v| {
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

fn socket_validator(v: String) -> Result<(), String> {
    match v.to_socket_addrs() {
        Ok(_) => Ok(()),
        Err(_) => Err(String::from(
            "Value does not specify a hostname:port value to bind to",
        )),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let matches = ClApp::new("PubSub Server")
        .version("0.1.0")
        .author(crate_authors!("\n"))
        .arg(
            Arg::with_name("bind")
                .short("b")
                .long("bind")
                .takes_value(true)
                .required(true)
                .validator(socket_validator),
        )
        .arg(
            Arg::with_name("publisher-timeout")
                .short("t")
                .takes_value(true)
                .help("How long after registration that publishers are valid in seconds")
                .default_value("300"),
        )
        .arg(
            Arg::with_name("publisher-scan-interval")
                .short("s")
                .takes_value(true)
                .help("How often are publishers scanned and possibly removed in seconds")
                .default_value("30"),
        )
        .get_matches();

    let scan_interval = matches.value_of("publisher-scan-interval").unwrap();
    let publisher_timeout = Duration::from_secs(
        matches
            .value_of("publisher-timeout")
            .unwrap()
            .parse()
            .unwrap(),
    );
    let bind_info = matches.value_of("bind").unwrap().parse().unwrap();
    let state = ProtectedState {
        inner: Arc::new(RwLock::new(ServerState::new())),
        publisher_timeout,
    };
    let state_for_cleaner = state.inner.clone();

    remove_expired_publishers(
        state_for_cleaner,
        Duration::from_secs(scan_interval.parse().unwrap()),
    );

    let server = Server::builder()
        .add_service(FindMeServer::new(state))
        .serve(bind_info);
    info!("Started server {}", bind_info);
    server.await?;

    Ok(())
}
