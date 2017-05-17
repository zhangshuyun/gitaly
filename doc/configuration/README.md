# Configuring Gitaly

This document describes how to configure the Gitaly server
application.

Gitaly is configured via a [TOML](https://github.com/toml-lang/toml)
configuration file. Where this TOML file is located and how you should
edit it depend on how you installed GitLab. See:
https://docs.gitlab.com/ce/administration/gitaly/

The configuration file is passed as an argument to the `gitaly`
executable. This is usually done by either omnibus-gitlab or your init
script.

```
gitaly /path/to/config.toml
```

## Format

```toml
socket_path = "/path/to/gitaly.sock"
listen_addr = ":8081"
prometheus_listen_addr = ":9236"

[[storage]]
path = "/path/to/storage/repositories"
name = "my_shard"

# Gitaly may serve from multiple storages
#[[storage]]
#name = "other_storage"
#path = "/path/to/other/repositories"
```

|name|type|required|notes|
|----|----|--------|-----|
|socket_path|string|see notes|A path which gitaly should open a Unix socket. Required unless listen_addr is set|
|listen_addr|string|see notes|TCP address for Gitaly to listen on (See #GITALY_LISTEN_ADDR). Required unless socket_path is set|
|prometheus_listen_addr|string|no|TCP listen address for Prometheus metrics. If not set, no Prometheus listener is started|
|storage|array|yes|An array of storage shards|

### Storage

GitLab repositories are grouped into 'storages'. These are directories
(e.g. `/home/git/repositories`) containing bare repositories managed
by GitLab , with names (e.g. `default`).

These names and paths are also defined in the `gitlab.yml`
configuration file of gitlab-ce (or gitlab-ee). When you run Gitaly on
the same machine as gitlab-ce, which is the default and recommended
configuration, storage paths defined in Gitaly's config.toml must
match those in gitlab.yml.

|name|type|required|notes|
|----|----|--------|-----|
|path|string|yes|Path to storage shard|
|name|string|yes|Name of storage shard|

## Environment variables

### GITALY_DEBUG

When set to `1`, Gitaly will print debug log messages.

### GITALY_SOCKET_PATH

Required unless GITALY_LISTEN_ADDR is set. Overrides `socket_path` in
config.toml. Deprecated; use config.toml.

A path at which Gitaly should open a Unix socket. Example value:

```
GITALY_SOCKET_PATH=/home/git/gitlab/tmp/sockets/private/gitaly.socket
```

### GITALY_LISTEN_ADDR

Required unless GITALY_SOCKET_PATH is set. Overrides `listen_addr` in
config.toml. Deprecated; use config.toml.

TCP address for Gitaly to listen on. Note: at the moment Gitaly does
not offer any form of authentication. When you use a TCP listener you
must use firewalls or other network-based security to restrict access
to Gitaly.

Example value:

```
GITALY_LISTEN_ADDR=localhost:1234
```

### GITALY_PROMETHEUS_LISTEN_ADDR

Optional. Overrides `prometheus_listen_addr` in config.toml.
Deprecated; use config.toml.

TCP listen address for Prometheus metrics. When missing or empty, no
Prometheus listener is started.

```
GITALY_PROMETHEUS_LISTEN_ADDR=localhost:9236
```
