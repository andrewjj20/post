#[macro_use]
extern crate log;

use tests::COMMON_ENV;

#[test]
fn test() {
    env_logger::init();
    COMMON_ENV.enter(async {
        COMMON_ENV
            .find
            .server_status()
            .await
            .expect("Service status should not return error");
    })
}
