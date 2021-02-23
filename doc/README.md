## Gitaly documentation

The historical reasons for the inception of Gitaly and our design decisions are
written in [the design doc](doc/DESIGN.md).

#### Configuring Gitaly

Running Gitaly requires it to be configured correctly, options are described in
GitLab's  [configuration documentation](https://gitlab.com/gitlab-org/gitlab/blob/master/doc/administration/gitaly/index.md).

The reference guide is documented in https://gitlab.com/gitlab-org/gitlab/blob/master/doc/administration/gitaly/reference.md.

#### Developing Gitaly

- When new to Gitaly development, start by reading the [beginners guide](doc/beginners_guide.md)
- When developing on Gitaly-Ruby, read the [Gitaly-Ruby doc](doc/ruby_endpoint.md)
- The Gitaly release process is described in [our process doc](doc/PROCESS.md)
- Tests use Git repositories too, [read more about them](doc/test_repos.md)
- Praefect uses SQL. To create a new SQL migration see [sql_migrations.md](sql_migrations.md)
- For Gitaly hooks documentation, see [Gitaly hooks documentation](hooks.md)

#### Gitaly Cluster

Gitaly does not replicate any data. If a Gitaly server goes down, any of its
clients can't read or write to the repositories stored on that server. This
means that Gitaly is not highly available. How this will be solved is described
[in the HA design document](doc/design_ha.md)

For configuration please read [praefects configuration documentation](doc/configuration/praefect.md).

#### Technical explanations

- [Delta Islands](delta_islands.md)
- [Disk-based Cache](design_diskcache.md)
- [gitaly-ssh](../cmd/gitaly-ssh/README.md)
- [Git object quarantine during git push](object_quarantine.md)
- [Logging in Gitaly](logging.md)
- [Tips for reading Git source code](reading_git_source.md)
- [Serverside Git Usage](serverside_git_usage.md)
- [Object Pools](object_pools.md)

#### RFCs

- [Praefect Queue storage](rfcs/praefect-queue-storage.md)
- [Snapshot storage](rfcs/snapshot-storage.md)
