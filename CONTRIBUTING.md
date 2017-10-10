## Contributing

Thank you for your interest in contributing to the Gitaly project! We welcome
all contributions. By participating in this project, you agree to abide by the
[code of conduct](#code-of-conduct).

## Contributor license agreement

By submitting code as an individual you agree to the [individual contributor
license agreement](https://docs.gitlab.com/ee/legal/individual_contributor_license_agreement.html).

By submitting code as an entity you agree to the [corporate contributor license
agreement](https://docs.gitlab.com/ee/legal/corporate_contributor_license_agreement.html).

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

This Code of Conduct is adapted from the [Contributor Covenant](http://contributor-covenant.org), version 1.1.0,
available at [http://contributor-covenant.org/version/1/1/0/](http://contributor-covenant.org/version/1/1/0/).

## Style Guide

The Gitaly style guide is [documented in it's own file](STYLE.md)

## Changelog

Any new merge request must contain either a CHANGELOG.md entry or a
justification why no changelog entry is needed. New changelog entries
should be added to the **top** of the 'UNRELEASED' section of CHANGELOG.md.

If a change is specific to an RPC, start the changelog line with the
RPC name. So for a change to RPC `FooBar` you would get:

> FooBar: Add support for `fluffy_bunnies` parameter

## GitLab CE changelog

We only create GitLab CE changelog entries for two types of merge request:

- adding a feature flag for one or more Gitaly RPC's (meaning the RPC becomes available for trials)
- removing a feature flag for one or more Gitaly RPC's (meaning everybody is using a given RPC from that point on)

## Gitaly Maintainers

| Maintainer         |
|--------------------|
|@jacobvosmaer-gitlab|


## Development Process

We use long-running "~Conversation" issues (aka meta-issues) to track the progress of endpoint migrations and other work.

These issues can be tracked on the **[Migration Board](https://gitlab.com/gitlab-org/gitaly/boards/331341)**.

Conversation issues help us track migrations through the stages of the [migration process](doc/MIGRATION_PROCESS.md):
- ~"Migration:Ready-for-Development"
- ~"Migration:RPC-Design"
- ~"Migration:Server-Impl"
- ~"Migration:Client-Impl"
- ~"Migration:Ready-for-Testing"
- ~"Migration:Acceptance-Testing"
- ~"Migration:Disabled"
- ~"Migration:Opt-In"
- ~"Migration:Opt-Out"
- ~"Migration:Mandatory"

While migration may take several releases from end-to-end, and, so that we can better track progress, each stage of the migration will
spawn it's own issue. These issues should generally not stay open for more than a week or two.

## How to develop a migration

1. **Select a migration endpoint**: select a migration from the [migration board](https://gitlab.com/gitlab-org/gitaly/boards/331341). These migrations are prioritised so choose one near the top.
   - Assign the conversation issue to yourself so that others know that you are working on it.
1. **Build the RPC interface**:
   - Using the RPC design in the issue, create a merge request in the [gitaly-proto](https://gitlab.com/gitlab-org/gitaly-proto) repo
1. **Build the Gitaly Server implementation**
1. **Build the client implementation**: this will be in one or more of GitLab-Workhorse, GitLab-Shell, GitLab-CE and GitLab-EE.
1. **Note**: you may find it more productive to work on the RPC interface, server and client implementations simultaneously
   - Sometimes while building the server and client you may discover problems with the RPC interface
   - For this reason, it can be easier to vendor your branch of `gitaly-proto` into your server and client branches during development
   - In GitLab-CE and GitLab-Shell, custom versions of Gitaly can be specified in the `GITALY_SERVER_VERSION` file by using the syntax
     `=my-branch-name`
   - Remember to release a new version of `gitaly-proto` and `gitaly` and change the `GITALY_SERVER_VERSIONS` before review.
1. **Await a new release of GitLab**:
   - Assign the conversation to the corresponding ["Post GitLab <Version> Deployment"](https://gitlab.com/gitlab-org/gitaly/milestones/)
     milestone.
1. **Perform Acceptance Testing**
   - We use feature toggles to gradually enable migration endpoints while monitoring their performance and status
   - If a critical bug is discovered, it's disable the feature toggle and move the conversation into the ~"Migration:Disabled" columm
     on the [migration board](https://gitlab.com/gitlab-org/gitaly/boards/331341) until a fix has been released.

### Workflow

1. The owner of a migration is responsible for moving conversation issues across the
   [migration board](https://gitlab.com/gitlab-org/gitaly/boards/331341).
1. The Gitaly team uses a weekly Wednesday-Wednesday iteration with retrospectives every second week.
1. Work for the cycle should be assigned to the [appropriate Infrastructure Deliverable](https://gitlab.com/gitlab-org/gitaly/milestones/) milestone and tagged with the ~"Infrastructure Deliverable" label.
   - Please **do not assign ~Conversation issues** to the weekly infrastructure deliverable milestone or the  ~"Infrastructure Deliverable" label.
   - ~"Conversation"s are ongoing work-streams while ~"Infrastructure Deliverable" issues should be closed by the upcoming milestone.
1. To keep track of slipping issues, items which we have been unable to complete by the infrastructure deliverable milestone should be moved over to the next milestone and marked with ~"Moved:x1",  ~"Moved:x2",  ~"Moved:x3" etc

## Reviews and Approvals

Merge requests need to **approval by at least two [Gitaly team members](https://gitlab.com/groups/gl-gitaly/group_members)**.

If a merge request will affect code that cannot be conditionally disabled via [feature flags](https://gitlab.com/gitlab-org/gitlab-ce/merge_requests/11747) then at least one of the approvers should be a **Gitaly maintainer**.

Examples of code that **requires maintainer approval** include:
* Configuration management
* Process startup or shutdown
* Shared utility classes such as stream utilities, etc

Examples of code that **does not require maintainer approval** include:
* Build script changes (does not run in production)
* Testing changes (does not run in production)
* Migration endpoints (can be disabled via a feature flag)

Additionally, if you feel that the change you are making is sufficiently complicated or just for your own confidence,
please feel free to assign a maintainer.

### Review Process

The Gitaly team uses the following process:

- When you merge request is ready for review, select two approvers from the Merge Request edit view.
- Assign the first reviewer
- When the first reviewer is done, they assign the second reviewer
- When the second reviewer is done
  - If there are no discussions, they are free to merge
  - Otherwise assign back to the author for next round of review.

**Note**: the author-reviewer 1-reviewer 2-author cycle works best with small changes. With larger changes feel free to use
the traditional author-reviewer 1-author-reviewer 1-reviewer 2-author-reviewer 2-cycle.

## Gitaly Developer Quick-start Guide

- Check the [README.md](README.md) file to ensure that you have the correct version of Go installed.
- To **lint**, **check formatting**, etc: `make verify`
- To **build**: `make build`
- To run **tests**: `make test`
- To get **coverage**: `make cover`
- **Clean**: `make clean`
- All-in-one: `make verify build test`

## Debug Logging

Debug logging can be enabled in Gitaly through the `GITALY_DEBUG` environment variable:

```
$ GITALY_DEBUG=1 ./gitaly config.toml
```

## Git Tracing

Gitaly will reexport `GIT_TRACE*` [environment variables](https://git-scm.com/book/en/v2/Git-Internals-Environment-Variables) if they are set.

This can be an aid to debugging some sets of problems. For example, if you would like to know what git is going internally, you can set `GIT_TRACE=true`:

Note that since git stderr stream will be logged at debug level, you need to enable debug logging too.

```shell
$ GITALY_DEBUG=1 GIT_TRACE=true ./gitaly config.toml
...
DEBU[0015] 13:04:08.646399 git.c:322               trace: built-in: git 'gc'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.request.topLevelGroup=gitlab grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.649346 run-command.c:626       trace: run_command: 'pack-refs' '--all' '--prune'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.request.topLevelGroup=gitlab grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.652240 git.c:322               trace: built-in: git 'pack-refs' '--all' '--prune'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.request.topLevelGroup=gitlab grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.655497 run-command.c:626       trace: run_command: 'reflog' 'expire' '--all'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.request.topLevelGroup=gitlab grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.658331 git.c:322               trace: built-in: git 'reflog' 'expire' '--all'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.request.topLevelGroup=gitlab grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.669787 run-command.c:626       trace: run_command: 'repack' '-d' '-l' '-A' '--unpack-unreachable=2.weeks.ago'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.request.topLevelGroup=gitlab grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.672589 git.c:322               trace: built-in: git 'repack' '-d' '-l' '-A' '--unpack-unreachable=2.weeks.ago'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.request.topLevelGroup=gitlab grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.673185 run-command.c:626       trace: run_command: 'pack-objects' '--keep-true-parents' '--non-empty' '--all' '--reflog' '--indexed-objects' '--write-bitmap-index' '--unpack-unreachable=2.weeks.ago' '--local' '--delta-base-offset' 'objects/pack/.tmp-60361-pack'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.request.topLevelGroup=gitlab grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.675955 git.c:322               trace: built-in: git 'pack-objects' '--keep-true-parents' '--non-empty' '--all' '--reflog' '--indexed-objects' '--write-bitmap-index' '--unpack-unreachable=2.weeks.ago' '--local' '--delta-base-offset' 'objects/pack/.tmp-60361-pack'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.request.topLevelGroup=gitlab grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0037] 13:04:30.737687 run-command.c:626       trace: run_command: 'prune' '--expire' '2.weeks.ago'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.request.topLevelGroup=gitlab grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0037] 13:04:30.753856 git.c:322               trace: built-in: git 'prune' '--expire' '2.weeks.ago'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.request.topLevelGroup=gitlab grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0037] 13:04:31.071140 run-command.c:626       trace: run_command: 'worktree' 'prune' '--expire' '3.months.ago'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.request.topLevelGroup=gitlab grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0037] 13:04:31.074736 git.c:322               trace: built-in: git 'worktree' 'prune' '--expire' '3.months.ago'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.request.topLevelGroup=gitlab grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0037] 13:04:31.076135 run-command.c:626       trace: run_command: 'rerere' 'gc'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.request.topLevelGroup=gitlab grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0037] 13:04:31.079286 git.c:322               trace: built-in: git 'rerere' 'gc'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.request.topLevelGroup=gitlab grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
```
