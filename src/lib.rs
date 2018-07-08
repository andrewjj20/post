#[macro_use]
extern crate serde_derive;
extern crate serde;

use std::time;

#[derive(Serialize, Deserialize, Debug)]
pub struct PublisherDesc {
    pub name: String,
    pub host_name: String,
    pub port: u16,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ConnectionInfo {
    pub name: String,
    pub last_report: time::SystemTime,
}

#[cfg(test)]
mod tests {}
