extern crate log;

use clap::{crate_authors, crate_version, App as ClApp, Arg};
use futures::{
    sink::SinkExt,
    stream::{StreamExt, TryStreamExt},
};
use pubsub::publisher::Publisher;
use std::error::Error as StdError;
use std::time::Duration;
use tokio_util::codec;

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    env_logger::init();

    let matches = ClApp::new("stdin_publisher")
        .version(crate_version!())
        .author(crate_authors!("\n"))
        .arg(
            Arg::with_name("url")
                .short("u")
                .long("url")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("subscriber_timeout")
                .short("t")
                .long("subscriber-timeout")
                .takes_value(true)
                .default_value("30")
                .required(true),
        )
        .get_matches();

    let url = String::from(matches.value_of("url").unwrap());
    let client = pubsub::find_service::Client::from_url(url)?
        .connect()
        .await?;
    let name = "stdin".to_string();
    let host_name = matches.value_of("host").unwrap().to_string();
    let port = matches
        .value_of("port")
        .unwrap()
        .parse()
        .expect("invalid integer in port");
    let subscriber_timeout = matches
        .value_of("subscriber_timeout")
        .unwrap()
        .parse()
        .expect("invalid integer in port");
    let publisher = Publisher::new(
        name,
        host_name,
        port,
        Duration::new(subscriber_timeout, 0),
        client,
    )
    .await?;
    codec::FramedRead::new(tokio::io::stdin(), codec::LinesCodec::new())
        .map_ok(bytes::Bytes::from)
        .map_err(Box::<dyn StdError>::from)
        .forward(SinkExt::<&[u8]>::sink_map_err(
            publisher,
            Box::<dyn StdError>::from,
        ))
        .await?;
    Ok(())
}
