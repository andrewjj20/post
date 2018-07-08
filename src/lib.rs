#[macro_use]
extern crate serde_derive;
extern crate serde;
mod framing;

use std::time;

#[derive(Serialize, Deserialize, Debug)]
pub struct PublisherDesc {
    pub name: String,
    pub host_name: String,
    pub port: u16,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ConnectionInfo {
    pub last_report: time::SystemTime,
}

pub struct Publisher {}

pub struct Subscription {}

#[cfg(test)]
mod tests {}
