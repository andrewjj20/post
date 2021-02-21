extern crate tokio;
use clap::{crate_authors, App as ClApp, Arg, ArgGroup};
use futures::future;
use futures::StreamExt;
use log::*;
use std::time;
use std::{collections::HashMap, result::Result, sync::RwLock};
use std::{convert::TryInto, net::ToSocketAddrs};
use time::Duration;

use post::find_service::{
    hash_map_publisher_store::HashMapPublisherStore,
    proto::find_me_server::FindMeServer,
    server::{MeetupServer, MeetupServerOptions, PublisherStore},
};
use tonic::transport::Server;

use std::os::raw::{c_int, c_void};
use std::os::unix::io as unix_io;
use std::os::unix::io::FromRawFd;

#[derive(Debug)]
pub struct InvalidSocketDescriptor {
    fd: unix_io::RawFd,
}

impl InvalidSocketDescriptor {
    pub fn new(fd: unix_io::RawFd) -> InvalidSocketDescriptor {
        InvalidSocketDescriptor { fd }
    }
}

impl std::fmt::Display for InvalidSocketDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Invalid Socket File Descriptor: {}", self.fd)
    }
}

impl std::error::Error for InvalidSocketDescriptor {}

unsafe fn getsocketopt<T>(fd: unix_io::RawFd, level: c_int, opt: c_int) -> std::io::Result<T> {
    let mut val: T = std::mem::zeroed();
    let mut val_size: libc::socklen_t = std::mem::size_of::<T>() as u32;
    let ret = libc::getsockopt(
        fd,
        level,
        opt,
        &mut val as *mut T as *mut c_void,
        &mut val_size as *mut libc::socklen_t,
    );
    if ret < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(val)
    }
}

fn listener_from_raw(
    fd: unix_io::RawFd,
) -> Result<std::net::TcpListener, Box<dyn std::error::Error>> {
    let family: c_int = unsafe { getsocketopt(fd, libc::SOL_SOCKET, libc::SO_DOMAIN) }?;

    if family != libc::AF_INET && family != libc::AF_INET6 {
        eprint!("Unable to use socket of family: {}", family);
        return Err(Box::new(InvalidSocketDescriptor::new(fd)));
    }

    let sock_type: c_int = unsafe { getsocketopt(fd, libc::SOL_SOCKET, libc::SO_TYPE) }?;

    if sock_type != libc::SOCK_STREAM {
        return Err(Box::new(InvalidSocketDescriptor::new(fd)));
    }

    Ok(unsafe { std::net::TcpListener::from_raw_fd(fd) })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let matches = ClApp::new("PubSub Server")
        .version("0.1.0")
        .author(crate_authors!("\n"))
        .arg(
            Arg::with_name("bind")
                .short("b")
                .long("bind")
                .takes_value(true)
                .validator(socket_validator)
                .help("IP and port to bind to in the form of <IP>:<Port>. Use 0.0.0.0 for any IP on the system."),
        )
        .arg(
            Arg::with_name("fd")
            .long("fd")
            .takes_value(true)
            .help("File descriptor of bound TCP socket ready for `listen` to be called on it")
        )
        .group(
            ArgGroup::with_name("Socket description")
            .args(&["bind","fd"])
            .required(true)
            )
        .arg(
            Arg::with_name("publisher-timeout")
                .short("t")
                .takes_value(true)
                .help("How long after registration that publishers are valid in seconds")
                .default_value("300"),
        )
        .arg(
            Arg::with_name("publisher-scan-interval")
                .short("s")
                .takes_value(true)
                .help("How often are publishers scanned and possibly removed in seconds")
                .default_value("30"),
        )
        .get_matches();

    let publisher_timeout = Duration::from_secs(
        matches
            .value_of("publisher-timeout")
            .unwrap()
            .parse()
            .unwrap(),
    );

    let scan_interval = Duration::from_secs(
        matches
            .value_of("publisher-scan-interval")
            .unwrap()
            .parse()
            .unwrap(),
    );
    let listener = match matches.value_of("bind") {
        Some(addr) => {
            tokio::net::TcpListener::bind(
                std::net::ToSocketAddrs::to_socket_addrs(addr)?
                    .next()
                    .expect("No Address found"),
            )
            .await?
        }
        None => tokio::net::TcpListener::from_std(listener_from_raw(
            matches.value_of("fd").unwrap().parse::<c_int>().unwrap(),
        )?)?,
    };
    let local_address = listener.local_addr()?;

    let publisher_store = HashMapPublisherStore::new(RwLock::new(HashMap::new()));
    let meetup_server_options = MeetupServerOptions {
        publisher_timeout: publisher_timeout,
        publisher_store: publisher_store.clone(),
    };

    let meetup_server = MeetupServer::new(meetup_server_options);

    let server = Server::builder()
        .add_service(FindMeServer::new(meetup_server))
        .serve_with_incoming(listener);

    info!("server listening at: {}", local_address);

    remove_expired_publishers(publisher_store, scan_interval);
    server.await?;

    Ok(())
}

fn remove_expired_publishers(publisher_store: HashMapPublisherStore, i: time::Duration) {
    tokio::spawn(tokio::time::interval(i).for_each(move |_| {
        let now = time::SystemTime::now();
        let publishers = publisher_store.get_publishers();

        let publishers_to_remove: Vec<String> = publishers
            .into_iter()
            .filter(|publisher_tuple| {
                let mut filter_out = true;
                if let Some(pub_info) = &publisher_tuple.1.info {
                    if let Some(expiration) = &pub_info.expiration {
                        match expiration.try_into() {
                            Result::<time::SystemTime, _>::Ok(proto_time) => {
                                filter_out = proto_time < now;
                                debug!("checking time proto_time: {:?}, now: {:?}, should filter out: {:?}", proto_time, now, filter_out);
                            }
                            Err(error) => {
                                error!("Removing descriptor, Invalid time: {}", error);
                            }
                        };
                    } else {
                        error!("Removing descriptor, No Expiration");
                    }
                } else {
                    error!("Removing descriptor, No ConnectionInfo");
                }
                filter_out
            })
            .map(|publisher_tuple| publisher_tuple.0)
            .collect();

        publisher_store.remove_publishers(&publishers_to_remove);

        future::ready(())
    }));
}

pub fn socket_validator(v: String) -> Result<(), String> {
    match v.to_socket_addrs() {
        Ok(_) => Ok(()),
        Err(_) => Err(String::from(
            "Value does not specify a hostname:port value to bind to",
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::io::AsRawFd;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn test_bad_fd() {
        let orig_listener = std::net::UdpSocket::bind(("127.0.0.1", 0))
            .expect("Unable to make a udp socket bound to localhost");
        let fd = orig_listener.as_raw_fd();

        let _listener =
            super::listener_from_raw(fd).expect_err("Sent invalid socket, recieved success");
    }

    #[test]
    fn test_good_fd() {
        let orig_listener = std::net::TcpListener::bind(("127.0.0.1", 0))
            .expect("Unable to make a tcp socket bound to localhost");
        let fd = orig_listener.as_raw_fd();

        let _listener = super::listener_from_raw(fd).expect("Sent valid socket, recieved error");
    }
}
