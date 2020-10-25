/// tools that should be independent of any test and can be shared
/// The tokio runtime is an example.
mod find_service_setup;
pub mod verify;

use find_service_setup::FindService;

pub struct CommonTestEnvironment {
    pub find: find_service_setup::FindService,
}

pub async fn setup() -> CommonTestEnvironment {
    env_logger::init();
    eprintln!("path:{:?}", std::env::current_dir().unwrap());

    let find = FindService::new().await;
    CommonTestEnvironment { find }
}

pub fn teardown() {}
