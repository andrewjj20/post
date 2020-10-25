use post::find_service::Client;

///Wraps an external find service process and provides easy access to its functions
pub struct FindService {
    _proc: tokio::process::Child,
    client: post::find_service::Client,
}

pub async fn retry_client(url: &'static str) -> post::find_service::Client {
    let retries: i32 = 10;
    let mut retry: i32 = 0;

    loop {
        if retry >= retries {
            panic!("Retries exceeded");
        }
        if let Ok(mut client) = post::find_service::Client::from_url(url)
            .unwrap()
            .set_connect_timeout(std::time::Duration::from_secs(60))
            .connect()
            .await
        {
            log::info!("client works, checking status");
            if client.server_status().await.is_ok() {
                break client;
            }
        }
        log::info!("Client retry");
        tokio::time::delay_for(tokio::time::Duration::from_millis(500)).await;
        retry += 1;
    }
}

impl FindService {
    pub async fn new() -> FindService {
        log::info!("Starting new meetup service");
        let path = "target/debug/post-meetup";
        let url = "http://127.0.0.1:8080/";
        let bind = "127.0.0.1:8080";

        let _proc = tokio::process::Command::new(path)
            .arg("--bind")
            .arg(bind)
            .kill_on_drop(true)
            .spawn()
            .expect("Failed to start meetup");

        log::info!("meetup service started");
        //tokio::runtime::Handle::current().block_on(tokio::time::delay_for(std::time::Duration::from_secs(1)));

        let client = retry_client(url).await;

        FindService { _proc, client }
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }
}
