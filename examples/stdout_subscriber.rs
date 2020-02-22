#[macro_use]
extern crate log;

use clap::{crate_authors, crate_version, App as ClApp, Arg};
use futures::future::{FutureExt, TryFutureExt};
use futures01::{future, Future, Stream};
use pubsub::find_service;
use pubsub::subscriber::Subscription;
use std::convert::TryInto;

fn main() {
    env_logger::init();

    let matches = ClApp::new("stdin_Publisher")
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

    let base_url = matches.value_of("url").unwrap().to_string();
    tokio::run(
        find_service::get_descriptors_for_name(base_url, "stdin".to_string())
            .boxed()
            .compat()
            .map_err(|e| {
                error!("Error retrieving descriptor {}", e);
            })
            .and_then(|resp| {
                let desc = resp
                    .list
                    .into_iter()
                    .next()
                    .expect("No Publisher found")
                    .publisher
                    .expect("Registration without description")
                    .try_into()
                    .expect("Conversion from proto to regular description failed");
                info!("Found descriptor {}", desc);
                future::result(Subscription::new(desc)).map_err(|e| {
                    error!("Error in subscribing: {}", e);
                })
            })
            .and_then(|sub| {
                sub.filter_map(|v| match String::from_utf8(v) {
                    Ok(s) => Some(s),
                    Err(e) => {
                        error!("UTF Error {}", e);
                        None
                    }
                })
                .forward(tokio_codec::FramedWrite::new(
                    tokio::io::stdout(),
                    tokio_codec::LinesCodec::new(),
                ))
                .map_err(|e| {
                    error!("Sink Error {}", e);
                })
                .map(|_| ())
            }),
    );
}
