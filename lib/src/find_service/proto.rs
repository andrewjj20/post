tonic::include_proto!("post");

use std::convert;
use std::error::Error;
use std::fmt;
use std::time::*;

/// An error that can occur when converting [Time] to [SystemTime]
#[derive(Debug)]
pub struct TimeError {
    /// The time struct that caused the error.
    pub time: Time,
}

impl fmt::Display for TimeError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            fmt,
            "Error when converting Time to SystemTime. Value Causing Problem: {:?}",
            self.time
        )
    }
}

impl Error for TimeError {}

impl convert::TryInto<SystemTime> for Time {
    type Error = TimeError;
    fn try_into(self) -> Result<SystemTime, Self::Error> {
        match SystemTime::UNIX_EPOCH.checked_add(Duration::new(self.secs, 0)) {
            Some(t) => Ok(t),
            None => Err(TimeError { time: self }),
        }
    }
}

impl convert::TryInto<SystemTime> for &Time {
    type Error = TimeError;
    fn try_into(self) -> Result<SystemTime, Self::Error> {
        match SystemTime::UNIX_EPOCH.checked_add(Duration::new(self.secs, 0)) {
            Some(t) => Ok(t),
            None => Err(TimeError { time: self.clone() }),
        }
    }
}

impl convert::TryFrom<SystemTime> for Time {
    type Error = SystemTimeError;
    fn try_from(time: SystemTime) -> Result<Self, Self::Error> {
        let duration = time.duration_since(SystemTime::UNIX_EPOCH)?;
        Ok(Self {
            secs: duration.as_secs(),
        })
    }
}

impl convert::Into<Duration> for TimeInterval {
    fn into(self) -> Duration {
        Duration::new(self.secs, 0)
    }
}

impl convert::From<Duration> for TimeInterval {
    fn from(duration: Duration) -> TimeInterval {
        TimeInterval {
            secs: duration.as_secs(),
        }
    }
}

impl convert::From<crate::PublisherDesc> for PublisherDesc {
    fn from(desc: crate::PublisherDesc) -> PublisherDesc {
        let crate::PublisherDesc {
            name,
            host_name,
            port,
            subscriber_expiration_interval,
        } = desc;
        PublisherDesc {
            name,
            host_name,
            port: port.into(),
            subscriber_expiration_interval: Some(subscriber_expiration_interval.into()),
        }
    }
}
