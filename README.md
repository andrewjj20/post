# pubsub

A Publish Subscribe library allowing multiple hosts to register, and subscribe services accross an IP network.

Goals:

* Offload tasks to the network where possible.
* Allow trade offs between latency and reliability to be determined by subscribers.
* Allow for the discovery of publishers.

## Supported Rust Version

Nightly rust is required. Last tested on:
```
rustc 1.48.0-nightly (043f6d747 2020-09-25)
```

## License

This project is licensed under the [MIT license].

[MIT license]: https://github.com/andrewjj20/pubsub/blob/master/LICENSE
