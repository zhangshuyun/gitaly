# Configuring Praefect

This document describes how to configure the praefect server.

Praefect is configured via a [TOML](https://github.com/toml-lang/toml)
configuration file. The TOML file contents and location depends on how you
installed GitLab. See: https://docs.gitlab.com/ce/administration/gitaly/

The configuration file is passed as an argument to the `praefect`
executable. This is usually done by either omnibus-gitlab or your init
script.

```
praefect -config /path/to/config.toml
```

## Format

```toml
listen_addr = "127.0.0.1:2305"

# socket_path = "/path/to/praefect.socket"
failover_enabled = true

[logging]
format = "json"
level = "info"

[[virtual_storage]]
name = "default"

[[virtual_storage.node]]
  name = "internal_storage_0"
  address = "tcp://localhost:9999"
  primary = true
  token = "supersecret"

[[virtual_storage.node]]
  name = "internal_storage_1"
  address = "tcp://localhost:9998"
  token = "supersecret"
```

An example [config toml](config.praefect.toml) is stored in this repository.

