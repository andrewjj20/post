#![feature(rust_2018_preview, use_extern_macros)]

use actix_web::State as ActixState;
use actix_web::{
    http::Method, middleware::Logger, server, App, AsyncResponder, Error, HttpMessage, HttpRequest,
    HttpResponse, Path,
};
use clap::{crate_authors, App as ClApp, Arg};
use futures::future::Future;
use log::*;
use pubsub::find_service::{ConnectionResponse, PubSubResponse, ServiceStatus};
use pubsub::{ConnectionInfo, PublisherDesc};
use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::option::Option;
use std::sync::{Arc, RwLock};
use std::time;
use tokio::prelude::*;

struct ServerState {
    publishers: HashMap<String, ConnectionResponse>,
    publisher_timeout: time::Duration,
}

impl ServerState {
    pub fn new(publisher_timeout: time::Duration) -> ServerState {
        ServerState {
            publishers: HashMap::new(),
            publisher_timeout,
        }
    }
}

type State = ServerState;
type ProtectedState = Arc<RwLock<State>>;

fn handle_service_status_request(req: &HttpRequest<ProtectedState>) -> HttpResponse {
    let state = req.state().read().unwrap();
    let status = ServiceStatus {
        count: state.publishers.len(),
    };
    HttpResponse::Ok().json(PubSubResponse::new(String::from("Success"), &status))
}

fn handle_add_connection(
    req: &HttpRequest<ProtectedState>,
) -> Box<Future<Item = HttpResponse, Error = Error>> {
    let protected_state = Arc::clone(req.state());
    req.json()
        .from_err()
        .and_then(move |publisher: PublisherDesc| {
            let mut state = protected_state.write().unwrap();
            let now = time::SystemTime::now();
            let info = ConnectionResponse {
                publisher: publisher,
                info: ConnectionInfo {
                    last_report: now,
                    expiration: now + state.publisher_timeout,
                },
            };
            let name = info.publisher.name.clone();
            state.publishers.insert(name.clone(), info);
            debug!("Added Connection \"{}\"", name);
            Ok(HttpResponse::Ok().json(PubSubResponse::registration(
                String::from("success"),
                state.publisher_timeout,
            )))
        })
        .responder()
}

fn handle_get_connection(info: (ActixState<ProtectedState>, Path<String>)) -> HttpResponse {
    let (prot_state, name) = info;
    let state = prot_state.read().unwrap();
    match state.publishers.get(&*name) {
        Option::None => {
            debug!("Did not find \"{}\"", *name);
            HttpResponse::NotFound().json(PubSubResponse::status_only(String::from("not found")))
        }
        Option::Some(e) => HttpResponse::Ok().json(PubSubResponse::new(String::from("success"), e)),
    }
}

fn handle_get_connections(req: &HttpRequest<ProtectedState>) -> HttpResponse {
    let state = req.state().read().unwrap();
    info!("returning connections");
    HttpResponse::Ok().json(PubSubResponse::new(
        String::from("success"),
        &state.publishers,
    ))
}

fn remove_expired_publishers(s: ProtectedState, i: time::Duration) {
    actix::Arbiter::spawn(
        tokio::timer::Interval::new(time::Instant::now() + i, i)
            .for_each(move |_| {
                let now = time::SystemTime::now();
                s.write()
                    .unwrap()
                    .publishers
                    .retain(|_k, v| v.info.expiration > now);
                Ok(())
            })
            .map_err(move |e| {
                panic!("Error: {}", e);
            }),
    );
}

fn socket_validator(v: String) -> Result<(), String> {
    match v.to_socket_addrs() {
        Ok(_) => Ok(()),
        Err(_) => Err(String::from(
            "Value does not specify a hostname:port value to bind to",
        )),
    }
}

fn main() {
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
    let publisher_timeout = matches.value_of("publisher-timeout").unwrap();
    let sys = actix::System::new("example");
    let bind_info = matches.value_of("bind").unwrap();
    let state = Arc::new(RwLock::new(State::new(time::Duration::from_secs(
        publisher_timeout.parse().unwrap(),
    ))));
    let state_for_cleaner = state.clone();

    server::new(move || {
        let state_clone = state.clone();
        App::<ProtectedState>::with_state(state_clone)
            .resource("/status", move |r| {
                r.method(Method::GET).h(handle_service_status_request)
            })
            .resource("/publishers/", move |r| {
                r.post().f(handle_add_connection);
                r.get().f(handle_get_connections);
            })
            .resource("/publishers/{name}", move |r| {
                r.method(Method::GET).with(handle_get_connection)
            })
            .middleware(Logger::default())
    }).bind(bind_info)
        .expect("Can not bind")
        .start();

    remove_expired_publishers(
        state_for_cleaner,
        time::Duration::from_secs(scan_interval.parse().unwrap()),
    );
    info!("Started server {}", bind_info);
    sys.run();
}
