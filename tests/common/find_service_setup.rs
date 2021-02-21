use post::find_service::Client;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;

///Wraps an external find service process and provides easy access to its functions
pub struct FindService {
    _proc: tokio::process::Child,
    client: post::find_service::Client,
}

fn unset_close_on_exec(fd: RawFd) -> std::io::Result<RawFd> {
    use std::io;
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags == -1 {
        return Err(io::Error::last_os_error());
    }
    let was_set = flags & libc::FD_CLOEXEC != 0;
    log::info!("State of Close on exec: {}", was_set);
    let result = unsafe { libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(fd)
    }
}

pub async fn retry_client(url: String) -> post::find_service::Client {
    let retries: i32 = 10;
    let mut retry: i32 = 0;

    loop {
        if retry >= retries {
            panic!("Retries exceeded");
        }
        if let Ok(mut client) = post::find_service::Client::from_url(url.clone())
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

        let listener = std::net::TcpListener::bind(("127.0.0.1", 0))
            .expect("could not reserve address for find service");
        let port = listener
            .local_addr()
            .expect("could not retrieve port from OS for find service")
            .port();
        let listener_fd =
            unset_close_on_exec(listener.as_raw_fd()).expect("could not disable close on exec");

        log::info!("meetup server starting on port {}", port);
        let url = format!("http://127.0.0.1:{}/", port);

        let _proc = tokio::process::Command::new(path)
            .arg("-s")
            .arg("5")
            .arg("-t")
            .arg("5")
            .arg("--fd")
            .arg(format!("{}", listener_fd))
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
