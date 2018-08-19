#![feature(rust_2018_preview, use_extern_macros)]

#[macro_use]
extern crate log;

use clap::{crate_authors, crate_version, App as ClApp, Arg};
use futures::{future, Future, Stream};
use pubsub::find_service;
use pubsub::subscriber::Subscription;

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

    let base_url = matches.value_of("url").unwrap();
    tokio::run(
        find_service::get_descriptors_for_name(base_url, "stdin")
            .map_err(|e| {
                error!("Error retreiving descriptor {}", e);
            })
            .and_then(|resp| {
                let desc = resp.response.publisher;
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
                }).forward(tokio_codec::FramedWrite::new(
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
