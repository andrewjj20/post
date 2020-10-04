extern crate log;

#[macro_use]
use crate::verify::{self, VerificationStatus, Verifier};
use futures::{sink::SinkExt, stream::StreamExt, TryFutureExt};
use std::sync::Arc;

use crate::common;

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
    client
        .server_status()
        .await
        .expect("Service status should not return error");

    let desc = pubsub::PublisherDesc {
        name: "basic".to_string(),
        host_name: "127.0.0.1".to_string(),
        port: 5000,
        subscriber_expiration_interval: std::time::Duration::from_secs(5),
    };

    let verifier = Arc::new(verify::ConstantVerifier::new(0xaaaaaaaaaaaaaaaa_u64));
    let send_verifier = verifier.clone();
    let receive_verifier = verifier.clone();

    let mut publisher = pubsub::publisher::Publisher::from_description(desc.clone(), client)
        .await
        .expect("Unable to create Publisher");

    let mut subscriber = pubsub::subscriber::Subscription::new(desc.clone())
        .await
        .expect("Unable to create Subscription");

    println!("yeeters");
    debug!("publisher and subscriber initialized");

    subscriber.wait_for_subscrition_complete().await;

    debug!("Subscription active");

    let receive_one = tokio::spawn(async {
        let verified = verify::VerifiedStream::new(subscriber, receive_verifier);
        let message = verified.into_future().await.0;
        debug!("Received");
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
        debug!("sent");
        ret
    }
    .map_err(GeneralError::from);

    let (send_result, receive_result) = futures::future::join(send_one, receive_one).await;

    debug!("done");

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
