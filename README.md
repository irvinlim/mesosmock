# mesosmock

**mesosmock** is a HTTP server aimed to mock the behaviour of a real Apache Mesos master, without setting up a real Mesos cluster with several agents. This can be useful for integration/end-to-end tests, or stress/chaos testing of Mesos frameworks.

The server can be configured to respond or initiate requests based on a configuration file. For example, taking the scheduler API, resource offers and status updates can be configured to be sent at a specific rate.

## APIs Supported

The following APIs are currently supported by mesosmock:

- [Scheduler HTTP API (v1)](https://mesos.apache.org/documentation/latest/scheduler-http-api/)

The following are also targeted to be supported by mesosmock:

- [Operator HTTP API (v1)](https://mesos.apache.org/documentation/latest/operator-http-api/)
- [Master HTTP API](https://mesos.apache.org/documentation/latest/endpoints/)

Older Protobuf APIs (v0) will not be supported.

## Usage

```sh
./mesosmock -config config.json -listen 5050
```

### Command-line Options

TODO

### Configuration

TODO
