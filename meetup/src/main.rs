extern crate tokio;
use clap::{crate_authors, App as ClApp, Arg};
use log::*;
use std::net::ToSocketAddrs;
use std::result::Result;
use std::time;
use time::Duration;

use post::find_service::proto::find_me_server::FindMeServer;

use meetup_server::{MeetupServer, MeetupServerOptions};
use tonic::transport::Server;

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
                .validator(socket_validator)
                .help("IP and port to bind to in the form of <IP>:<Port>. Use 0.0.0.0 for any IP on the system."),
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

    let publisher_timeout = Duration::from_secs(
        matches
            .value_of("publisher-timeout")
            .unwrap()
            .parse()
            .unwrap(),
    );

    let scan_interval = Duration::from_secs(
        matches
            .value_of("publisher-scan-interval")
            .unwrap()
            .parse()
            .unwrap(),
    );
    let bind_info = matches.value_of("bind").unwrap().parse().unwrap();

    let meetup_server_options = MeetupServerOptions {
        publisher_timeout: publisher_timeout,
        publisher_scan_interval: scan_interval,
    };
    let meetup_server = MeetupServer::new(meetup_server_options);
    meetup_server.start_remove_process();

    let server = Server::builder()
        .add_service(FindMeServer::new(meetup_server))
        .serve(bind_info);

    info!("Started server {}", bind_info);
    server.await?;

    Ok(())
}

pub fn socket_validator(v: String) -> Result<(), String> {
    match v.to_socket_addrs() {
        Ok(_) => Ok(()),
        Err(_) => Err(String::from(
            "Value does not specify a hostname:port value to bind to",
        )),
    }
}
