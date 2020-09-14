//! Test utilities for pubsub

#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;

pub mod find;
pub mod verify;

/// tools that should be independent of any test and can be shared
/// The tokio runtime is an example.
pub struct CommonTestEnvironment {
    pub runtime: tokio::runtime::Runtime,
    pub find: find::FindService,
}

impl CommonTestEnvironment {
    pub fn new() -> CommonTestEnvironment {
        let runtime = tokio::runtime::Builder::new()
            .enable_all()
            .threaded_scheduler()
            .thread_name("pubsub-test-worker")
            .build()
            .expect("Unable to start runtime");
        let handle = runtime.handle().clone();
        let find = runtime
            .handle()
            .block_on(async { find::FindService::new(handle).await });
        CommonTestEnvironment { runtime, find }
    }
    pub fn enter<F>(&self, f: F) -> F::Output
    where
        F: std::future::Future,
    {
        self.runtime.handle().block_on(f)
    }
}

impl Default for CommonTestEnvironment {
    fn default() -> CommonTestEnvironment {
        CommonTestEnvironment::new()
    }
}

lazy_static! {
    pub static ref COMMON_ENV: CommonTestEnvironment = {
        env_logger::init();
        eprintln!("path:{:?}", std::env::current_dir().unwrap());
        CommonTestEnvironment::new()
    };
}
