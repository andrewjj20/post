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

type State = HashMap<String, ConnectionResponse>;
type ProtectedState = Arc<RwLock<State>>;

fn handle_service_status_request(req: HttpRequest<ProtectedState>) -> HttpResponse {
    let state = req.state().read().unwrap();
    let status = ServiceStatus { count: state.len() };
    HttpResponse::Ok().json(PubSubResponse::new(String::from("Success"), &status))
}

fn handle_add_connection(
    req: HttpRequest<ProtectedState>,
) -> Box<Future<Item = HttpResponse, Error = Error>> {
    let protected_state = Arc::clone(req.state());
    req.json()
        .from_err()
        .and_then(move |publisher: PublisherDesc| {
            let info = ConnectionResponse {
                publisher: publisher,
                info: ConnectionInfo {
                    last_report: time::SystemTime::now(),
                },
            };
            let mut state = protected_state.write().unwrap();
            state.insert(info.publisher.name.clone(), info);
            Ok(HttpResponse::Ok().json(PubSubResponse::status_only(String::from("success"))))
        })
        .responder()
}

fn handle_get_connection(info: (ActixState<ProtectedState>, Path<String>)) -> HttpResponse {
    let (prot_state, name) = info;
    let state = prot_state.read().unwrap();
    match state.get(&*name) {
        Option::None => {
            HttpResponse::NotFound().json(PubSubResponse::status_only(String::from("not found")))
        }
        Option::Some(e) => HttpResponse::Ok().json(PubSubResponse::new(String::from("success"), e)),
    }
}

fn handle_get_connections(req: HttpRequest<ProtectedState>) -> HttpResponse {
    let state = req.state().read().unwrap();
    info!("returning connections");
    HttpResponse::Ok().json(PubSubResponse::new(String::from("success"), &*state))
}

fn actix_pubsub_app() -> App<ProtectedState> {
    let state = Arc::new(RwLock::new(State::new()));
    App::<ProtectedState>::with_state(state)
        .resource("/status", move |r| {
            r.method(Method::GET).h(handle_service_status_request)
        })
        .resource("/publishers", move |r| {
            r.post().f(handle_add_connection);
            r.get().f(handle_get_connections);
        })
        .resource("/publishers/{name}", move |r| {
            r.method(Method::GET).with(handle_get_connection)
        })
        .middleware(Logger::default())
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
        .get_matches();

    let sys = actix::System::new("example");
    let bind_info = matches.value_of("bind").unwrap();

    server::new(actix_pubsub_app)
        .bind(bind_info)
        .expect("Can not bind")
        .start();

    info!("Started server {}", bind_info);
    sys.run();
}
