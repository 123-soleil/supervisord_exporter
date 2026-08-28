# Supervisor Exporter

The Supervisor Exporter is a simple Go application that collects process status information from the Supervisor process control system and exposes it as Prometheus metrics. This allows you to monitor the state of processes managed by Supervisor.

## Table of Contents

- [Features](#features)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Prometheus Metrics](#prometheus-metrics)
- [License](#license)

## Features

- Collects process status information from Supervisor.
- Exposes process state, exit status, and more as Prometheus metrics.
- Configurable via command line parameters.
- Provides a simple HTTP server for Prometheus to scrape metrics.
- Handles unreachable Supervisord XML-RPC endpoints gracefully.
- Supports both HTTP and Unix socket connections to Supervisord.
- Supports authentication with username and password.

## Getting Started

### Prerequisites

Before running the Supervisor Exporter, make sure you have the following prerequisites:

- Ensure that the Supervisord instance is configured to expose the XML-RPC interface. 

Follow these steps to enable the XML-RPC endpoint on Supervisord:

1. **Edit Supervisord Configuration:**

   Open the configuration file for Supervisord, typically located at `/etc/supervisord.conf` or a custom path specified during installation. You can use your preferred text editor to edit the file. For example:

   ```shell
   vi /etc/supervisord.conf
   ```
2. **Configure the XML-RPC Server:**

   Add the following lines to your Supervisord configuration file, if they are not already present, to configure the XML-RPC server.

   **Option 1: TCP/HTTP Server (inet_http_server):**
   
   ```shell
   [inet_http_server]
   port = 127.0.0.1:9001
   username = dummy
   password = dummy
   ```

   * `port`: Specify the IP address and port where the XML-RPC server will listen. In the example above, it listens on 127.0.0.1:9001.
   * `username` and `password`: Optional authentication credentials.

   **Option 2: Unix Socket Server (unix_http_server):**
   
   ```shell
   [unix_http_server]
   file=/run/supervisord/supervisor.sock
   username = dummy
   password = dummy
   ```

   * `file`: Specify the path to the Unix socket file.
   * `username` and `password`: Optional authentication credentials.

3. **Save and Restart Supervisord:**
   Save the configuration file and then restart Supervisord to apply the changes:

   ```shell
   supervisorctl reread
   supervisorctl update
   ```

Once you have configured and verified the XML-RPC endpoint on Supervisord, you can use the Supervisor Exporter to monitor your processes using Prometheus.

4. **Verify XML-RPC Endpoint:**

   To ensure the XML-RPC endpoint is working, you can test it by using a tool like curl or accessing it in your web browser:
   ```shell
   curl http://127.0.0.1:9001/RPC2
   ```

### Installation

1. Clone the repository:

   ```shell
   git clone https://github.com/123-soleil/supervisord_exporter.git
   ```

2. Build the application:
   ```shell
   go build
   ```

### Usage

To start the Supervisord Exporter, run the following command:

   ```shell
   ./supervisord_exporter
   ```

By default, the exporter will listen on port 9876 and use the Supervisor XML-RPC interface at `http://localhost:9001/RPC2`. You can change the defaults using command line parameters (see [Configuration](#configuration) section).

### Configuration

The Supervisord Exporter can be configured using command line parameters. Here are the available parameters:

* `-supervisord-url`: The URL of the Supervisord XML-RPC interface. Supports both HTTP and Unix socket schemes. Default is `http://localhost:9001/RPC2`
  * HTTP example: `http://localhost:9001/RPC2`
  * Unix socket example: `unix:///run/supervisord/supervisor.sock`
* `-username`: Username for Supervisord authentication (optional). Prefer the `SUPERVISORD_USERNAME` environment variable, since CLI flags are visible to other local users via the process list.
* `-password`: Password for Supervisord authentication (optional). Prefer the `SUPERVISORD_PASSWORD` environment variable, since CLI flags are visible to other local users via the process list.
* `-supervisord-timeout`: Timeout for XML-RPC requests to Supervisord. Default is `10s`
* `-stale-grace-period`: How long to keep serving the last known process metrics (with `supervisord_up=0`) after Supervisord becomes unreachable, before clearing them as too stale to trust. Default is `1m`
* `-web.listen-address`: The address and port where the exporter will listen for HTTP requests. Default is `:9876`
* `-web.telemetry-path`: Path under which to expose metrics. Default is `/metrics`
* `-version`: Print the version information and exit.

Examples of custom configurations:

**HTTP with authentication:**
```shell
./supervisord_exporter -supervisord-url="http://example.com:9001/RPC2" -username="dummy" -password="dummy" -web.listen-address=":8080"
```

**Unix socket with authentication:**
```shell
./supervisord_exporter -supervisord-url="unix:///run/supervisord/supervisor.sock" -username="dummy" -password="dummy"
```

**Unix socket without authentication:**
```shell
./supervisord_exporter -supervisord-url="unix:///var/run/supervisor.sock"
```

**Credentials via environment variables (recommended):**
```shell
SUPERVISORD_USERNAME="dummy" SUPERVISORD_PASSWORD="dummy" ./supervisord_exporter -supervisord-url="http://example.com:9001/RPC2"
```

### Prometheus Metrics

The Supervisord Exporter exposes the following Prometheus metrics:

* `supervisor_process_info`: Gauge vector with labels for `name`, `group`, `state`, and `exit_status`. Value is `1` if the process is running (`state="RUNNING"`) and `0` otherwise. `exit_status` is `0` for a running process, and the process's actual exit code otherwise.
* `supervisor_process_uptime`: Gauge vector with labels `name` and `group`, giving the uptime in seconds of a running process. Only exported for processes currently in the `RUNNING` state.
* `supervisord_up`: Gauge metric indicating the status of the connection to Supervisord (1 if up, 0 if down).
* `supervisor_last_successful_scrape_timestamp_seconds`: Unix timestamp of the last successful Supervisord XML-RPC scrape. Use this to detect stale data independently of `supervisord_up`.

If the Supervisord XML-RPC endpoint becomes unreachable, `supervisord_up` drops to 0 immediately, but `supervisor_process_info`/`supervisor_process_uptime` keep reporting the last known values for up to `-stale-grace-period` (default `1m`) so a brief hiccup doesn't make every process's metrics disappear. If the outage outlasts that grace period, those metrics are cleared, since serving arbitrarily old process/uptime data during a real outage would be misleading.


Sample metric:
```
supervisor_process_info{exit_status="0",group="apache2",name="apache2",state="RUNNING"} 1
supervisord_up{} 1
```

### License

This project is licensed under the MIT License

