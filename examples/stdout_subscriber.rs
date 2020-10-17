#![feature(async_closure)]

extern crate log;

use clap::{crate_authors, crate_version, App as ClApp, Arg};
use futures::{
    sink::SinkExt,
    stream::{StreamExt, TryStreamExt},
};
use post::subscriber::Subscription;
use std::convert::TryInto;
use std::error::Error as StdError;
use tokio_util::codec;

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
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
    let mut client = post::find_service::Client::from_url(base_url)?
        .connect()
        .await?;
    let desc = client
        .get_descriptors_for_name("stdin".to_string())
        .await?
        .list
        .into_iter()
        .next()
        .expect("No Publisher found")
        .publisher
        .expect("Registration without description")
        .try_into()
        .expect("Conversion from proto to regular description failed");
    let sub = Subscription::new(desc).await?;
    sub.map(|b| String::from_utf8(std::convert::From::from(b.as_ref())))
        .map_err(Box::<dyn StdError>::from)
        .forward(SinkExt::<String>::sink_map_err(
            codec::FramedWrite::new(tokio::io::stdout(), codec::LinesCodec::new()),
            Box::<dyn StdError>::from,
        ))
        .await
}
