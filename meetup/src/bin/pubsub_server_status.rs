extern crate tokio;
use clap::{crate_authors, crate_version, App as ClApp, Arg, SubCommand};
use log::*;
use pubsub::find_service;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
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
        .subcommand(SubCommand::with_name("service").about("Retrieves the high level status"))
        .subcommand(SubCommand::with_name("publishers").about("Displays a list of publishers"))
        .get_matches();

    let base_url = matches
        .value_of("url")
        .ok_or_else(|| eyre::Report::msg("No URL provided"))?;
    let mut client = find_service::Client::from_url(base_url)?.connect().await?;

    info!("Starting Request");
    match matches.subcommand() {
        ("service", Some(_service_matches)) => {
            let status = client.server_status().await?;
            info!("Status: {:?}", status);
            Ok(())
        }
        ("publishers", Some(_publisers_matches)) => {
            let descriptors = client.get_descriptors().await?;
            info!("response received {:?}", descriptors);
            Ok(())
        }
        (_, None) => {
            println!("Subcommand not found");
            Ok(())
        }
        _ => unreachable!(),
    }
}
