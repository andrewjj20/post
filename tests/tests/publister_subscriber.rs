#[macro_use]
extern crate log;

use futures::{
    future::{FutureExt, TryFutureExt},
    sink::SinkExt,
    stream::StreamExt,
};
use std::sync::Arc;
use tests::verify::{self, VerificationStatus, Verifier};
use tests::COMMON_ENV;

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

#[test]
///Send a message, see if it was received.
fn publisher_subscriber_basics() {
    COMMON_ENV.enter(async {
        let mut client = COMMON_ENV.find.client();
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
        .map_err(|e| GeneralError::from(e));

        let send_one = async {
            let message = send_verifier.create_message();
            let ret = publisher.send(message).await;
            debug!("sent");
            ret
        }
        .map_err(|e| GeneralError::from(e));

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
    })
}
