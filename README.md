# post

A Publish Subscribe library allowing multiple hosts to register, and subscribe services accross an IP network.

![Crates.io](https://img.shields.io/crates/v/post)
![GitHub Workflow Status (branch)](https://img.shields.io/github/workflow/status/andrewjj20/post/Rust/master)
![Crates.io](https://img.shields.io/crates/l/post)

Goals:

* Offload tasks to the network where possible.
* Allow trade offs between latency and reliability to be determined by subscribers.
* Allow for the discovery of publishers.

## Supported Rust Version

Tested with the latest stable as of the commit date of the latest commit.

## License

This project is licensed under the [MIT license].

[MIT license]: https://github.com/andrewjj20/post/blob/master/LICENSE

## Examples

 * [Publisher](https://github.com/andrewjj20/post/blob/master/examples/stdin_publisher.rs)
 * [Subscriber](https://github.com/andrewjj20/post/blob/master/examples/stdout_subscriber.rs)

### Running exmples
 * Start the meetup server. This provides a central location for publishers and subscribers to find each other. A Subscriber can connect to a Publisher without it.
   ```
   cargo run --bin post-meetup -- --bind 127.0.0.1:8080
   ```
 * Start the publisher:
   ```
   cargo run --example stdin_publisher -- --host 127.0.0.1 --port 8081 --url http://127.0.0.1:8080
   ```
 * Start the subscriber:
   ```
   cargo run --example stdout_subscriber -- --url http://127.0.0.1:8080
   ```
 * Text typed on the publisher is now being sent to the subscriber
