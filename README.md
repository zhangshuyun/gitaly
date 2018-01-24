# ![Gitaly](https://gitlab.com/gitlab-org/gitaly/uploads/509123ed56bd51247996038c858db006/gitaly-wordmark-small.png)

[![Pipeline status](https://gitlab.com/gitlab-org/gitaly/badges/master/pipeline.svg)](https://gitlab.com/gitlab-org/gitaly/commits/master) [![coverage report](https://gitlab.com/gitlab-org/gitaly/badges/master/coverage.svg)](https://codecov.io/gl/gitlab-org/gitaly)

**Quick Links**:
  [**Migration Board**](https://gitlab.com/gitlab-org/gitaly/boards/331341?scope=all&utf8=%E2%9C%93&state=opened&label_name[]=Migration) |
  [**Roadmap**](doc/ROADMAP.md) |
  [Open Conversations](https://gitlab.com/gitlab-org/gitaly/issues?label_name%5B%5D=Conversation) |
  [Unassigned Conversations](https://gitlab.com/gitlab-org/gitaly/issues?scope=all&utf8=%E2%9C%93&state=opened&label_name[]=Conversation&label_name[]=To%20Do&assignee_id=0) |
  [Migrations](https://gitlab.com/gitlab-org/gitaly/issues?scope=all&utf8=%E2%9C%93&state=opened&label_name[]=Conversation&label_name[]=Migration) |
  [Want to Contribute?](https://gitlab.com/gitlab-org/gitaly/issues?scope=all&utf8=%E2%9C%93&state=opened&label_name[]=Accepting%20Merge%20Requests) |
  [GitLab Gitaly Issues](https://gitlab.com/groups/gitlab-org/issues?scope=all&state=opened&utf8=%E2%9C%93&label_name%5B%5D=Gitaly) |
  [GitLab Gitaly Merge Requests](https://gitlab.com/groups/gitlab-org/merge_requests?label_name%5B%5D=Gitaly) |
  [gitlab.com dashboard](https://performance.gitlab.net/dashboard/db/gitaly) |

--------------------------------------------

Gitaly is a Git [RPC](https://en.wikipedia.org/wiki/Remote_procedure_call)
service for handling all the git calls made by GitLab.

To see where it fits in please look at [GitLab's architecture](https://docs.gitlab.com/ce/development/architecture.html#system-layout)

Gitaly is still under development. We expect it to become a standard
component of GitLab in Q1 2017 and to reach full scope in Q3 2017.

## Project Goals

Make the git data storage tier of large GitLab instances, and *GitLab.com in particular*, **fast**.

This will be achieved by focusing on two areas (in this order):

  1. **Move git operations as close to the data as possible**
     * Migrate from git operations on workers, accessing git data over NFS to
       Gitaly services running on file-servers accessing git data on local
       drives ([See our test results](https://gitlab.com/gitlab-com/infrastructure/issues/1912#note_31368476))
     * Ultimately, this will lead to all git operations occurring via the Gitaly
       service and the removal of the need for NFS access to git volumes.
  1. **Optimize git services using caching and other techniques**

## Current Status

Gitaly has been shipped as part of GitLab since 9.0. We are migrating git operations from in-process Rugged implementations to Gitaly service endpoints.

[The roadmap is available here](doc/ROADMAP.md).

The migration process is [documented](/doc/MIGRATION_PROCESS.md).

If you're interested in seeing how well Gitaly is performing on GitLab.com, we have dashboards!

##### Overall

[![image](https://gitlab.com/gitlab-org/gitaly/uploads/ee1fd4f33e9bfb92fefca60fee1f44ad/image.png)](http://monitor.gitlab.net/dashboard/db/gitaly?orgId=1&var-job=gitaly-production&from=now-7d&to=now)

##### By Feature

 [![image](https://gitlab.com/gitlab-org/gitaly/uploads/5b3825e01c48975c2a64e01ae37b4a3d/image.png)](http://monitor.gitlab.net/dashboard/db/gitaly-features?orgId=1&var-job=gitaly-production&from=now-7d&to=now)

## Migrations

The progress of Gitaly's endpoint migrations is tracked via the [**Migration Board**](https://gitlab.com/gitlab-org/gitaly/boards/331341?scope=all&utf8=%E2%9C%93&state=opened&label_name[]=Migration)

## Installation

Gitaly requires Go 1.8 or newer and Ruby 2.3. Run `make` to download
and compile Ruby dependencies, and to compile the Gitaly Go
executable.

Gitaly uses `git`. Version `2.14.3` is recommended, and `2.9.0` at a minimum.

## Configuration

See [configuration documentation](doc/configuration)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Name

Gitaly is a tribute to git and the town of [Aly](https://en.wikipedia.org/wiki/Aly). Where the town of
Aly has zero inhabitants most of the year we would like to reduce the number of
disk operations to zero for most actions. It doesn't hurt that it sounds like
Italy, the capital of which is [the destination of all roads](https://en.wikipedia.org/wiki/All_roads_lead_to_Rome). All git actions in
GitLab end up in Gitaly.

## Design

High-level architecture overview:

![Gitaly Architecture](https://docs.google.com/drawings/d/14-5NHGvsOVaAJZl2w7pIli8iDUqed2eIbvXdff5jneo/pub?w=2096&h=1536)

[Edit this diagram directly in Google Drawings](https://docs.google.com/drawings/d/14-5NHGvsOVaAJZl2w7pIli8iDUqed2eIbvXdff5jneo/edit)

## Presentations

- [Git Paris meetup, 2017-02-22](https://docs.google.com/presentation/d/19OZUalFMIDM8WujXrrIyCuVb_oVeaUzpb-UdGThOvAo/edit?usp=sharing) a high-level overview of what our plans are and where we are.
- [Gitaly Basics, 2017-05-01](https://docs.google.com/presentation/d/1cLslUbXVkniOaeJ-r3s5AYF0kQep8VeNfvs0XSGrpA0/edit#slide=id.g1c73db867d_0_0)
- [Infrastructure Team Update 2017-05-11](https://about.gitlab.com/2017/05/11/functional-group-updates/#infrastructure-team)
