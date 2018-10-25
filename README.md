# mesosmock

**mesosmock** is a HTTP server aimed to mock the behaviour of a real Apache Mesos master, without setting up a real Mesos cluster with several agents. This can be useful for integration/end-to-end tests, or stress/chaos testing of Mesos frameworks.

The server can be configured to respond or initiate requests based on a configuration file. For example, taking the scheduler API, resource offers and status updates can be configured to be sent at a specific rate.

## APIs Supported

The following APIs are supported currently (or WIP) by `mesosmock`:

- [Scheduler HTTP API (v1)](https://mesos.apache.org/documentation/latest/scheduler-http-api/)
- [Operator HTTP API (v1)](https://mesos.apache.org/documentation/latest/operator-http-api/)
- [Master HTTP API](https://mesos.apache.org/documentation/latest/endpoints/)

Only JSON is supported at the moment. Additionally, the older v0 APIs will not be supported.

## Usage

```sh
./mesosmock -config=config.json
```

### Configuration

TODO


## Development

This project uses [govendor](https://github.com/kardianos/govendor).
