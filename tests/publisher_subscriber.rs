extern crate log;

pub mod common;

use common::verify::{self, VerificationStatus, Verifier};
use futures::{sink::SinkExt, stream::StreamExt, TryFutureExt};
use std::{convert::TryFrom, sync::Arc};

type GeneralError = Box<dyn std::error::Error>;

/*
#[test]
fn find_service() {
    COMMON_ENV.enter(async {
        COMMON_ENV
            .find
            .server_status()
            .await
            .expect("Service status should not return error");
    })
}
*/

#[tokio::test]
///Send a message, see if it was received.
async fn publisher_subscriber_basics() {
    let test_env = common::setup().await;
    let mut client = test_env.find.client();
    let publisher_name = "basic".to_string();
    client
        .server_status()
        .await
        .expect("Service status should not return error");

    let desc = post::PublisherDesc {
        name: publisher_name.clone(),
        host_name: "127.0.0.1".to_string(),
        port: 5000,
        subscriber_expiration_interval: std::time::Duration::from_secs(5),
    };

    let verifier = Arc::new(verify::ConstantVerifier::new(0xaaaaaaaaaaaaaaaa_u64));
    let send_verifier = verifier.clone();
    let receive_verifier = verifier.clone();

    log::debug!("Creating publisher");
    let mut publisher = post::publisher::Publisher::from_description(desc.clone(), client.clone())
        .await
        .expect("Unable to create Publisher");

    assert_ne!(
        client
            .server_status()
            .await
            .expect("Unable to retreive status")
            .count,
        0
    );

    log::debug!("Searching for publisher");
    let found_publisher = client
        .get_descriptors_for_name(publisher_name)
        .await
        .expect("Error finding publisher")
        .list
        .pop()
        .expect("No publisher found");

    assert_eq!(found_publisher.info.is_some(), true);

    let found_publisher_desc = post::PublisherDesc::try_from(
        found_publisher
            .publisher
            .expect("Publisher did not contain a description"),
    )
    .expect("Unable to convert returned description");

    let mut subscriber = post::subscriber::Subscription::new(found_publisher_desc)
        .await
        .expect("Unable to create Subscription");

    log::debug!("publisher and subscriber initialized");

    subscriber.wait_for_subscription_complete().await;

    log::debug!("Subscription active");

    let receive_one = tokio::spawn(async {
        let verified = verify::VerifiedStream::new(subscriber, receive_verifier);
        let message = verified.into_future().await.0;
        log::debug!("Received");
        match message {
            Some(m) => match m {
                VerificationStatus::Verified => Ok(()),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Error parsing message",
                )),
            },
            None => Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Subscription Closed, no data sent",
            )),
        }
    })
    .map_err(GeneralError::from);

    let send_one = async {
        let message = send_verifier.create_message();
        let ret = publisher.send(message).await;
        log::debug!("sent");
        ret
    }
    .map_err(GeneralError::from);

    let (send_result, receive_result) = futures::future::join(send_one, receive_one).await;

    log::debug!("done");

    if send_result.is_err() || receive_result.is_err() {
        panic!(
            "Send: {} Receive: {}",
            match send_result {
                Ok(_) => "Succeeded".to_string(),
                Err(e) => format!("{}", e),
            },
            match receive_result {
                Ok(_) => "Succeeded".to_string(),
                Err(e) => format!("{}", e),
            }
        );
    }

    common::teardown();
}

#[tokio::test]
///Send a message, see if it was received.
async fn publisher_cleanup() {
    let test_env = common::setup().await;

    let mut client = test_env.find.client();
    let publisher_name = "basic".to_string();
    client
        .server_status()
        .await
        .expect("Service status should not return error");

    let desc = post::PublisherDesc {
        name: publisher_name.clone(),
        host_name: "127.0.0.1".to_string(),
        port: 5000,
        subscriber_expiration_interval: std::time::Duration::from_secs(2),
    };

    log::debug!("Creating publisher");
    let mut _publisher = post::publisher::Publisher::from_description(desc.clone(), client.clone())
        .await
        .expect("Unable to create Publisher");

    log::debug!("Searching for publisher");
    let found_publisher = client
        .get_descriptors_for_name(publisher_name.clone())
        .await
        .expect("Error finding publisher")
        .list
        .pop()
        .expect("No publisher found");

    assert_eq!(found_publisher.info.is_some(), true);

    let found_publisher_desc = post::PublisherDesc::try_from(
        found_publisher
            .publisher
            .expect("Publisher did not contain a description"),
    )
    .expect("Unable to convert returned description");

    let mut subscriber = post::subscriber::Subscription::new(found_publisher_desc)
        .await
        .expect("Unable to create Subscription");

    log::debug!("publisher and subscriber initialized");

    subscriber.wait_for_subscription_complete().await;

    tokio::time::delay_for(std::time::Duration::from_secs(10)).await;

    let publishers_list = client
        .get_descriptors_for_name(publisher_name)
        .await
        .expect("Error retrieving publishers")
        .list;

    assert_eq!(publishers_list.len(), 0);

    common::teardown();
}
