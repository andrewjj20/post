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

    let base_url = matches.value_of("url").unwrap();

    info!("Starting Request");
    match matches.subcommand() {
        ("service", Some(_service_matches)) => {
            let status = find_service::server_status(base_url).await?;
            info!("Status: {:?}", status);
            Ok(())
        }
        ("publishers", Some(_publisers_matches)) => {
            let descriptors = find_service::get_descriptors(base_url).await?;
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
