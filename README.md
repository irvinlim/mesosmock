# Mesosmock

[![Analytics](https://ga-beacon.irvinlim.com/UA-61872435-6/mesosmock/readme)](https://github.com/irvinlim/ga-beacon)

_Mesosmock is currently still an active work in progress (WIP)._

**Mesosmock** aims to mock the behaviour of a real [Apache Mesos](https://mesos.apache.org/) cluster. Specifically, it is a HTTP server with the same API as a Mesos master, emulating various behaviours as if it is serving a real Mesos cluster. This can be useful for integration/end-to-end tests, or stress/chaos testing of [Mesos frameworks](https://mesos.apache.org/documentation/latest/frameworks/).

Mesosmock can be configured to behave differently based on settings loaded from a TOML configuration file. For example, you can mock an arbitrary number of Mesos agents with infinite resources, or emulate random task failures with a specific probability.

## Features

- Mock an entire Mesos cluster with a single Mesosmock process
  - Mock task executions without separate agents or executors
  - Mock any number of agents with any amount of resources
- Cluster emulation for chaos testing:
  - Emulate task failure or task loss (via Scheduler `UPDATE` events)
  - Emulate agent failure (via Operator `AGENT_ADDED` or Scheduler `TASK_LOST` updates) (WIP)
  - Emulate resource shortage (WIP)

## APIs Supported

The following APIs are supported currently (or will be eventually supported) by Mesosmock:

- [Scheduler HTTP API (v1)](https://mesos.apache.org/documentation/latest/scheduler-http-api/)
- [Operator HTTP API (v1)](https://mesos.apache.org/documentation/latest/operator-http-api/)
- [Master HTTP API](https://mesos.apache.org/documentation/latest/endpoints/)

Only JSON APIs are supported at the moment. Additionally, the older v0 APIs will not be supported.

## Usage

```sh
./mesosmock -config=config.json
```

### Command-line Flags

- `-config`: Path to config file. See `config.example.toml` for sample config file format.
- `-logLevel`: Global log level for Mesosmock. Logging may be especially expensive if there are many events and calls, reduce the level as necessary.

### Configuration Format

```toml
# IP address to listen on.
ip = "127.0.0.1"

# Port to listen on.
port = 5050

# Hostname to use for the Mesos master.
hostname = "localhost"

# Mesos mock cluster configuration.
[mesos]
  # Number of agents to simulate in the cluster.
  agent_count = 2

  # Amount of resources to offer each time.
  resources_cpu_offered = 64.0
  resources_mem_offered = 65536

# Configuration for cluster emulation.
[emulation]

  # Emulates tasks transiting to different TaskStates in their lifecycle.
  # All probability ratios are over 1; the sum of all ratios (excluding ratio_task_error and ratio_task_lost_recovered)
  # cannot exceed 1. Leftover ratio proportions will be the ratio of tasks that terminate in TASK_FINISHED.
  [emulation.task_state]

  # Delay (in seconds) before transiting to TASK_STAGING state.
  delay_task_staging = 0.0

  # Delay (in seconds) before transiting to TASK_STARTING state.
  delay_task_starting = 1.0

  # Delay (in seconds) before transiting to the next state.
  delay_task_next_state = 5.0

  # Ratio of tasks that terminate in TASK_FAILED.
  ratio_task_failed = 0.0

  # Ratio of tasks that terminate in TASK_ERROR.
  ratio_task_error = 0.0

  # Ratio of tasks that terminate in TASK_DROPPED.
  ratio_task_dropped = 0.0

  # Ratio of tasks that terminate in TASK_GONE.
  ratio_task_gone = 0.0

  # Ratio of tasks that terminate in TASK_GONE_BY_OPERATOR.
  ratio_task_gone_by_operator = 0.0

  # Ratio of tasks that terminate in TASK_UNREACHABLE.
  ratio_task_unreachable = 0.0

  # Ratio of tasks that transit to TASK_LOST state.
  ratio_task_lost = 0.0

  # Ratio of tasks that were lost that end up being recovered.
  # These recovered tasks will be subject to the same probability ratios as if it was never lost in the first place.
  # This is a ratio of the number of tasks that were lost, not of all tasks.
  # For example, if ratio_task_lost = 0.1 and ratio_task_lost_recovered = 0.5,
  # and 10 tasks out of 100 total tasks were lost, then 5 of those tasks will be recovered.
  ratio_task_lost_recovered = 0.0

  # Delay (in seconds) between recovery of lost tasks back to TASK_RUNNING.
  delay_task_lost_recovered = 10.0
```

## Development

This project uses [govendor](https://github.com/kardianos/govendor).

### Linux

To build for Linux (64-bit):

```sh
env GOOS=linux GOARCH=arm64 go build -v .
```

### Project Roadmap

- APIs
  - [ ] Scheduler v1 API
  - [ ] Operator v1 API
  - [ ] Master v1 API
- Emulation
  - [x] Cluster emulation for tasks
  - [ ] Cluster emulation for agents
- Development
  - [ ] Create unit tests
  - [ ] Set up CI/CD
  - [ ] Create Docker image for binary

## License

MIT
