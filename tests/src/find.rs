use pubsub::find_service::proto;
use std::error::Error;

///Wraps an external find service process and provides easy access to its functions
pub struct FindService {
    _proc: tokio::task::JoinHandle<std::result::Result<std::process::ExitStatus, std::io::Error>>,
    _handle: tokio::runtime::Handle,
    url: &'static str,
}

impl FindService {
    pub fn new(_handle: tokio::runtime::Handle) -> FindService {
        let path = "../target/debug/pubsub-meetup";
        let url = "http://127.0.0.1:8080/";
        let bind = "127.0.0.1:8080";

        let _proc = _handle.enter(|| {
            info!("Starting meetme service");
            let ret = tokio::spawn(
                tokio::process::Command::new(path)
                    .arg("--bind")
                    .arg(bind)
                    .kill_on_drop(true)
                    .spawn()
                    .expect("Failed to start meetup"),
            );

            info!("meetme service started");
            //tokio::runtime::Handle::current().block_on(tokio::time::delay_for(std::time::Duration::from_secs(1)));
            //info!("meetme service started");
            ret
        });

        FindService {
            _proc,
            _handle,
            url,
        }
    }

    pub fn url(&self) -> &'static str {
        self.url
    }

    pub async fn server_status(
        &self,
    ) -> std::result::Result<proto::StatusResponse, Box<dyn Error>> {
        pubsub::find_service::server_status(&self.url).await
    }
}
