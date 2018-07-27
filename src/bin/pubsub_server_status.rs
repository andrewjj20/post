#![feature(rust_2018_preview, use_extern_macros)]

use clap::{crate_authors, crate_version, App as ClApp, Arg};
use futures::future::Future;
use log::*;
use pubsub::find_service;
use tokio::prelude::*;

fn main() {
    env_logger::init();

    let matches = ClApp::new("PubSubServerStatus")
        .version(crate_version!())
        .author(crate_authors!("\n"))
        .arg(
            Arg::with_name("url")
                .short("u")
                .long("url")
                .required(true)
                .takes_value(true),
        )
        .get_matches();

    let base_url = matches.value_of("url").unwrap();

    let request = find_service::server_status(base_url)
        .map_err(|err| err.to_string())
        .and_then(|resp| {
            info!("response received");
            future::result(serde_json::to_string_pretty(&resp)).map_err(|err| err.to_string())
        })
        .then(|resp| {
            match resp {
                Err(e) => println!("Error: {}", e),
                Ok(json) => println!("{}", json),
            }
            future::ok::<(), ()>(())
        });
    info!("Starting Request");
    tokio::run(request);
}
