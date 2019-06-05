# Logging

The following values configure logging in Gitaly under the `[logging]` section

### dir

While main gitaly application logs go to stdout, there are extra log files that go to a configured directory. These include:

1. Gitlab shell logs

### format

The format of log messages. The current supported format is `json`. Any other value will be ignored and the default `text` format will be used.

### level

The log level. Valid values are the following:

1. `trace`
1. `debug`
1. `info`
1. `warn`
1. `error`
1. `fatal`
1. `panic`

The default log level is `info`.

#### Gitlab Shell Exceptions

Gitlab Shell does not spport `panic` or `trace`. `panic` will fall back to `error`, while `trace` will fall back to `debug`. Any other invalid log levels
will default to `info`.

## Format

```toml
[logging]
dir = "/home/gitaly/logs"
format = "json"
level = "info"
```