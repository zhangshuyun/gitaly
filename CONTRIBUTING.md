## Developer Certificate of Origin + License

By contributing to GitLab B.V., You accept and agree to the following terms and
conditions for Your present and future Contributions submitted to GitLab B.V.
Except for the license granted herein to GitLab B.V. and recipients of software
distributed by GitLab B.V., You reserve all right, title, and interest in and to
Your Contributions. All Contributions are subject to the following DCO + License
terms.

[DCO + License](https://gitlab.com/gitlab-org/dco/blob/master/README.md)

_This notice should stay as the first item in the CONTRIBUTING.md file._

## Code of conduct

As contributors and maintainers of this project, we pledge to respect all people
who contribute through reporting issues, posting feature requests, updating
documentation, submitting pull requests or patches, and other activities.

We are committed to making participation in this project a harassment-free
experience for everyone, regardless of level of experience, gender, gender
identity and expression, sexual orientation, disability, personal appearance,
body size, race, ethnicity, age, or religion.

Examples of unacceptable behavior by participants include the use of sexual
language or imagery, derogatory comments or personal attacks, trolling, public
or private harassment, insults, or other unprofessional conduct.

Project maintainers have the right and responsibility to remove, edit, or reject
comments, commits, code, wiki edits, issues, and other contributions that are
not aligned to this Code of Conduct. Project maintainers who do not follow the
Code of Conduct may be removed from the project team.

This code of conduct applies both within project spaces and in public spaces
when an individual is representing the project or its community.

Instances of abusive, harassing, or otherwise unacceptable behavior can be
reported by emailing contact@gitlab.com.

This Code of Conduct is adapted from the [Contributor Covenant](https://contributor-covenant.org), version 1.1.0,
available at [https://contributor-covenant.org/version/1/1/0/](https://contributor-covenant.org/version/1/1/0/).

## Style Guide

The Gitaly style guide is [documented in it's own file](STYLE.md).

## Changelog

Gitaly keeps a [changelog](CHANGELOG.md) which is generated when a new release
is created. The changelog is generated from entries that are included on each
merge request. To generate an entry on your branch run:
`_support/changelog "Change descriptions"`.

After the merge request is created, the ID of the merge request needs to be set
in the generated file. If you already know the merge request ID, run:
`_support/changelog -m <ID> "Change descriptions"`.

Any new merge request must contain either a new entry or a
justification in the merge request description why no changelog entry is needed.

If a change is specific to an RPC, start the changelog line with the
RPC name. So for a change to RPC `FooBar` you would get:

> FooBar: Add support for `fluffy_bunnies` parameter

## Gitaly Maintainers

| Maintainer         |
|--------------------|
|@8bitlife|
|@avar|
|@pks-t|
|@proglottis|
|@samihiltunen|
|@zj-gitlab|
|@toon|

## Development Process

Gitaly follows the engineering process as described in the [handbook][eng-process],
with the exception that our [issue tracker][gitaly-issues] is on the Gitaly
project and there's no distinction between developers and maintainers. Every team
member is equally responsible for a successful master pipeline and fixing security
issues.

Merge requests need to **approval by at least two
[Gitaly team members](https://gitlab.com/groups/gl-gitaly/group_members)**.

[eng-process]: https://about.gitlab.com/handbook/engineering/workflow/
[gitaly-issues]: https://gitlab.com/gitlab-org/gitaly/issues/

### Review Process

See [REVIEWING.md](REVIEWING.md).

## Gitaly Developer Quick-start Guide

See the [beginner's guide](doc/beginners_guide.md).

## Debug Logging

Debug logging can be enabled in Gitaly using `level = "debug"` under `[logging]` in config.toml.

## Git Tracing

Gitaly will reexport `GIT_TRACE*` [environment variables](https://git-scm.com/book/en/v2/Git-Internals-Environment-Variables) if they are set.

This can be an aid to debugging some sets of problems. For example, if you would like to know what git is doing internally, you can set `GIT_TRACE=true`:

Note that since git stderr stream will be logged at debug level, you need to enable debug logging in `config.toml`.

```shell
$ GIT_TRACE=true ./gitaly config.toml
...
DEBU[0015] 13:04:08.646399 git.c:322               trace: built-in: git 'gc'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.649346 run-command.c:626       trace: run_command: 'pack-refs' '--all' '--prune'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.652240 git.c:322               trace: built-in: git 'pack-refs' '--all' '--prune'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.655497 run-command.c:626       trace: run_command: 'reflog' 'expire' '--all'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.658331 git.c:322               trace: built-in: git 'reflog' 'expire' '--all'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.669787 run-command.c:626       trace: run_command: 'repack' '-d' '-l' '-A' '--unpack-unreachable=2.weeks.ago'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.672589 git.c:322               trace: built-in: git 'repack' '-d' '-l' '-A' '--unpack-unreachable=2.weeks.ago'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.673185 run-command.c:626       trace: run_command: 'pack-objects' '--keep-true-parents' '--non-empty' '--all' '--reflog' '--indexed-objects' '--write-bitmap-index' '--unpack-unreachable=2.weeks.ago' '--local' '--delta-base-offset' 'objects/pack/.tmp-60361-pack'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.675955 git.c:322               trace: built-in: git 'pack-objects' '--keep-true-parents' '--non-empty' '--all' '--reflog' '--indexed-objects' '--write-bitmap-index' '--unpack-unreachable=2.weeks.ago' '--local' '--delta-base-offset' 'objects/pack/.tmp-60361-pack'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0037] 13:04:30.737687 run-command.c:626       trace: run_command: 'prune' '--expire' '2.weeks.ago'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0037] 13:04:30.753856 git.c:322               trace: built-in: git 'prune' '--expire' '2.weeks.ago'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0037] 13:04:31.071140 run-command.c:626       trace: run_command: 'worktree' 'prune' '--expire' '3.months.ago'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0037] 13:04:31.074736 git.c:322               trace: built-in: git 'worktree' 'prune' '--expire' '3.months.ago'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0037] 13:04:31.076135 run-command.c:626       trace: run_command: 'rerere' 'gc'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0037] 13:04:31.079286 git.c:322               trace: built-in: git 'rerere' 'gc'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
```

## Testing with Instrumentation

If you would like to test with instrumentation and prometheus metrics, use the `instrumented-cluster` docker compose configuration in
`_support/instrumented-cluster`. This cluster will create several services:

|*Service*|*Endpoint*|
|---------|------|
| Gitaly | [http://localhost:9999](http://localhost:9999) |
| Gitaly Metrics and pprof | [http://localhost:9236](http://localhost:9236) |
| Prometheus | [http://localhost:9090](http://localhost:9090) |
| cAdvisor | [http://localhost:8080](http://localhost:8080) |
| Grafana | [http://localhost:3000](http://localhost:3000) use default login `admin`/`admin` |

The gitaly service uses the `gitlab/gitaly:latest` image, which you need to build using `make docker` before starting the cluster.

Once you have the `gitlab/gitaly:latest` image, start the cluster from the `_support/instrumented-cluster` directory using:

```shell
docker-compose up --remove-orphans
```

Enter `^C` to kill the cluster.

Note that the Gitaly service is intentionally limited to 50% CPU and 200MB of memory. This can be adjusted in the `docker-compose.yml` file.

Once the cluster has started, it will clone the `gitlab-org/gitlab-ce` repository, for testing purposes.

This can then be used for testing, using tools like [gitaly-bench](https://gitlab.com/gitlab-org/gitaly-bench):

```shell
gitaly-bench -concurrency 100 -repo gitlab-org/gitlab-ce.git find-all-branches
```

It can also be used with profiling tools, for example [go-torch](https://github.com/uber/go-torch) for generating flame graphs, as follows:

```shell
go-torch --url http://localhost:9236 -p > flamegraph.svg
```
