extern crate tokio;
use clap::{crate_authors, App as ClApp, Arg};
use futures::future;
use futures::StreamExt;
use log::*;
use std::result::Result;
use std::time;
use std::{convert::TryInto, net::ToSocketAddrs};
use time::Duration;

use post::find_service::{
    proto::find_me_server::FindMeServer,
    server::{MeetupServer, MeetupServerOptions, PublisherStore},
    vec_publisher_store::VecPublisherStore,
};
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

    let publisher_store = VecPublisherStore::new();
    let meetup_server_options = MeetupServerOptions {
        publisher_timeout: publisher_timeout,
        publisher_store: publisher_store.clone(),
    };

    let meetup_server = MeetupServer::new(meetup_server_options);

    let server = Server::builder()
        .add_service(FindMeServer::new(meetup_server))
        .serve(bind_info);

    info!("Started server {}", bind_info);
    remove_expired_publishers(publisher_store, scan_interval);
    server.await?;

    Ok(())
}

fn remove_expired_publishers(publisher_store: VecPublisherStore, i: time::Duration) {
    tokio::spawn(tokio::time::interval(i).for_each(move |_| {
        let now = time::SystemTime::now();
        let publishers = publisher_store.get_publishers();

        let publishers_to_remove: Vec<String> = publishers
            .into_iter()
            .filter(|publisher_tuple| {
                let filter_out;
                if let Some(pub_info) = &publisher_tuple.1.info {
                    if let Some(expiration) = &pub_info.expiration {
                        match expiration.try_into() {
                            Result::<time::SystemTime, _>::Ok(proto_time) => {
                                filter_out = proto_time > now
                            }
                            Err(error) => {
                                error!("Removing descriptor, Invalid time: {}", error);
                                filter_out = true;
                            }
                        };
                    } else {
                        error!("Removing descriptor, No Expiration");
                        filter_out = true;
                    }
                } else {
                    error!("Removing descriptor, No ConnectionInfo");
                    filter_out = true;
                }
                filter_out
            })
            .map(|publisher_tuple| publisher_tuple.0)
            .collect();

        publisher_store.remove_publishers(&publishers_to_remove);

        future::ready(())
    }));
}

pub fn socket_validator(v: String) -> Result<(), String> {
    match v.to_socket_addrs() {
        Ok(_) => Ok(()),
        Err(_) => Err(String::from(
            "Value does not specify a hostname:port value to bind to",
        )),
    }
}
