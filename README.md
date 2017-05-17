# Gitaly

- [What](#what)
- [Name](#name)
- [Reason](#reason)
- [Decisions](#decisions)
- [Iterate](#iterate)
- [Plan](#plan)
- [Presentations](#presentations)

## What

Gitaly is a Git [RPC](https://en.wikipedia.org/wiki/Remote_procedure_call)
service for handling all the git calls made by GitLab.

To see where it fits in please look at [GitLab's architecture](https://docs.gitlab.com/ce/development/architecture.html#system-layout)

Gitaly is still under development. We expect it to become a standard
component of GitLab in Q1 2017 and to reach full scope in Q3 2017.

### Project Goals

Make the git data storage tier of large GitLab instances, and *GitLab.com in particular*, fast.

This will be achieved by focusing on two areas (in this order):

  1. Move git operations as close to the data as possible
     * Migrate from git operations on workers, accessing git data over NFS to
       Gitaly services running on file-servers accessing git data on local
       drives
     * Ultimately, this will lead to all git operations occurring via the Gitaly
       service and the removal of the need for NFS access to git volumes.
  1. Optimize git services using caching

#### Scope

To maintain the focus of the project, the following subjects are out-of-scope for the moment:

1. Replication and high availability (including multi-master and active-active).

## References

- [GitHub diff pages](http://githubengineering.com/how-we-made-diff-pages-3x-faster/)
- [Bitbucket adaptive throttling](https://developer.atlassian.com/blog/2016/12/bitbucket-adaptive-throttling/)
- [Bitbucket caches](https://developer.atlassian.com/blog/2016/12/bitbucket-caches/)
- [GitHub Dgit (later Spokes)](http://githubengineering.com/introducing-dgit/)
- [GitHub Spokes (former Dgit)](http://githubengineering.com/building-resilience-in-spokes/)
- [Git Ketch](https://dev.eclipse.org/mhonarc/lists/jgit-dev/msg03073.html)
- [Lots of thinking in issue 2](https://gitlab.com/gitlab-org/gitaly/issues/2)
- [Git Pack Protocol Reference](https://github.com/git/git/blob/master/Documentation/technical/pack-protocol.txt)
- [Git Transfer Protocol internals](https://git-scm.com/book/en/v2/Git-Internals-Transfer-Protocols)
- [E3 Elastic Experiment Executor](https://bitbucket.org/atlassian/elastic-experiment-executor)

## Installation

Gitaly requires Go 1.5 or newer. To install into `/usr/local/bin`,
run:

```
make install
```

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

## Reason

For GitLab.com the [git access is slow](https://gitlab.com/gitlab-com/infrastructure/issues/351).

When looking at `Rugged::Repository.new` performance data we can see that our P99 spikes up to 30 wall seconds, while the CPU time keeps in the realm of the 15 milliseconds. Pointing at filesystem access as the culprit.

![rugged.new timings](doc/img/rugged-new-timings.png)

Our P99 access time to just create a `Rugged::Repository` object, which is loading and processing the git objects from disk, spikes over 30 seconds, making it basically unusable. We also saw that just walking through the branches of gitlab-ce requires 2.4 wall seconds.

We considered to move to metal to fix our problems with higher performaning hardware. But our users are using GitLab in the cloud so it should work great there. And this way the increased performance will benefit every GitLab user.

Gitaly will make our situation better in a few steps:

1. One central place to monitor operations
1. Performance improvements doing less and caching more
1. Move the git operations from the app to the file/git server with git rpc (routing git access over JSON HTTP calls)
1. Use Git ketch to allow active-active (push to a local server), and distributed read operations (read from a secondary). This is far in the future, we might also use a distributed key value store instead. See the [active-active issue](https://gitlab.com/gitlab-org/gitlab-ee/issues/1381). Until we are active active we can just use persistent storage on the cloud to shard, this eliminates the need for redundancy.

## Decisions

All design decision should be added here.

1. Why are we considering to use Git Ketch? It is open source, uses the git protocol itself, is made by experts in distributed systems (Google), and is as simple as we can think of. We have to accept that we'll have to run the JVM on the Git servers.
1. We'll keep using the existing sharding functionality in GitLab to be able to add new servers. Currently we can use it to have multiple file/git servers. Later we will need multiple Git Ketch clusters.
1. We need to get rid of NFS mounting at some point because one broken NFS server causes all the application servers to fail to the point where you can't even ssh in.
1. We want to move the git executable as close to the disk as possible to reduce latency, hence the need for git rpc to talk between the app server and git.
1. [Cached metadata is stored in Redis LRU](https://gitlab.com/gitlab-org/gitaly/issues/2#note_20157141)
1. [Cached payloads are stored in files](https://gitlab.com/gitlab-org/gitaly/issues/14) since Redis can't store large objects
1. Why not use GitLab Git? So workhorse and ssh access can use the same system. We need this to manage cache invalidation.
1. Why not make this a library for most users instead of a daemon/server?
    * Centralization: We need this new layer to be accessed and to share resources from multiple sources. A library is not fit for this end.
    * A library would have to be used in one of our current components, none of which seems ideal to take on this task:
        * gitlab-shell: return to the gitolite model? No.
        * Gitlab-workhorse: is now a proxy for Rails; would then become simultaneous proxy and backend service. Sounds confusing.
        * Unicorn: cannot handle slow requests
        * Sidekiq: can handle slow jobs but not requests
        * Combination workhorse+unicorn+sidekiq+gitlab-shell: this is hard to get right and slow to build even when you are an expert
    * With a library we will still need to keep the NFS shares mounted in the application hosts. That puts a hard stop to scale our storage because we need to keep multiplying the NFS mounts in all the workers.
1. Can we focus on instrumenting first before building Gitaly? Prometheus doesn't work with Unicorn.
1. How do we ship this quickly without affecting users? Behind a feature flag like we did with workhorse. We can update it independently in production.
1. How much memory will this use? Guess 50MB, we will save memory in the rails app, guess more in sidekiq (GBs but not sure), but initially more because more libraries are still loaded everywhere.
1. What packaging tool do we use? [Govendor because we like it more](https://gitlab.com/gitlab-org/gitaly/issues/15)
1. How will the networking work? A unix socket for git operations and TCP for monitoring. This prevents having to build out authentication at this early stage. https://gitlab.com/gitlab-org/gitaly/issues/16
1. We'll include the `/vendor` directory in source control https://gitlab.com/gitlab-org/gitaly/issues/18
1. We will use [E3 from BitBucket to measure performance closely in isolation](https://gitlab.com/gitlab-org/gitaly/issues/34).
1. Use environment variables for setting configurations (see #20).
1. GitLab already has [logic so that the application servers know which file/git server contains what repository](https://docs.gitlab.com/ee/administration/repository_storages.html), this eliminates the need for a router.
1. Use [gRPC](http://www.grpc.io/) instead of HTTP+JSON. Not so much for performance reasons (Protobuf is faster than JSON) but because gRPC is an RPC framework. With HTTP+JSON we have to invent our own framework; with gRPC we get a set of conventions to work with. This will allow us to move faster once we have learned how to use gRPC.
1. All protocol definitions and auto-generated gRPC client code will be in the gitaly repo. We can include the client code from the rest of the application as a Ruby gem / Go package / client executable as needed. This will make cross-repo versioning easier.
1. Gitaly will expose high-level Git operations, not low-level Git object/ref storage lookups. Many interesting Git operations involve an unbounded number of Git object lookups. For example, the number of Git object lookups needed to generate a diff depends on the number of changed files and how deep those files are in the repository directory structure. It is not feasible to make each of those Git object lookups a remote procedure call.
1. By default all Go packages in the Gitaly repository use the `/internal` directory, unless we explicitly want to export something. The only exception is the `/cmd` directory for executables.
1. GitLab requests should use as few Gitaly gRPC calls as possible. This means it is OK to move GitLab application logic into Gitaly when it saves us gRPC round trips.
1. Defining new gRPC calls is cheap. It is better to define a new 'high level' gRPC call and save gRPC round trips than to chain / combine 'low level' gRPC calls.

## Iterate

[More on the Gitaly Process here](doc/PROCESS.md)

Instead of moving everything to Gitaly and only then optimize performance we'll iterate so we quickly have results

The iteration process is as follows:

1. Move a specific set of functions from Rails to Gitaly without performance optimizations (needs to happen before release, there is a switch to use either Rails or Gitaly)
1. Measure their original performance
1. Try to improve the performance by reducing reads and/or caching
1. Measure the effect and if warrented try again
1. Remove the switch from Rails

Some examples of a specific set of functions:

- The initial one is discussed in https://gitlab.com/gitlab-org/gitaly/issues/13
- File cache for Git HTTP GET /info/refs https://gitlab.com/gitlab-org/gitaly/issues/17
- Getting the “title” of a commit so we can use it for Markdown references/links
- Loading a blob for syntax highlighting
- Getting statistics (branch count, tag count, etc), ideally without loading all kinds of Git references (which currently happens)
- Checking if a blob is binary, text, svg, etc
- Blob cache seems complicated https://gitlab.com/gitlab-org/gitaly/issues/14

Based on the  [daily overview dashboard](http://performance.gitlab.net/dashboard/db/daily-overview?panelId=14&fullscreen), we should tackle the routes in `gitlab-rails` in the following order:

### GitLab CE changelog

We only create GitLab CE changelog entries for two types of merge request:

- adding a feature flag for one or more Gitaly RPC's (meaning the RPC becomes available for trials)
- removing a feature flag for one or more Gitaly RPC's (meaning everybody is using a given RPC from that point on)

### Order of Migration

Using [data based on the](#generating-prioritization-data) [daily overview dashboard](http://performance.gitlab.net/dashboard/db/daily-overview?panelId=14&fullscreen),
we've prioritised the order in which we'll work through the `gitlab-rails` controllers
in descending order of **99% Cumulative Time** (that is `(number of calls) * (99% call time)`).

A [Google Spreadsheet](https://docs.google.com/spreadsheets/d/1MVjsbLIjBVryMxO0UhBWebrwXuqpbCz18ZtrThcSFFU/edit) is available
with these calculations.

|**Controller**|**~"Migration Analysis"**|**~"RPC Design"**|**~"Server Implementation"**|**~"Client Implementation"**|**~"Acceptance Testing"**|**Optim 1**|**Optim 2**|
|--------------|-------------------------|-----------------|----------------------------|----------------------------|-------------------------|-----------|-----------|
| [`SmartHTTP::InfoRefsUploadPack` and `SmartHTTP::InfoRefsReceivePack`](https://gitlab.com/gitlab-org/gitaly/issues/223) | #36 | - | | | | | |
| [`Diff::CommitDiff`](https://gitlab.com/gitlab-org/gitaly/issues/222) | #64 | #80 | #88 | #89| #209 | | |
| [`Ref::FindRefName` and `Commit::CommitIsAncestor`](https://gitlab.com/gitlab-org/gitaly/issues/221) | #66 | #81 | #86 | #87 | #210 | | |
| [`Ref::FindDefaultBranchName`, `Ref::FindAllBranchNames`, `Ref::FindAllTagNames`](https://gitlab.com/gitlab-org/gitaly/issues/220) | #65 | #82 | #84 | #85 | #211 | | |  
| [`SmartHTTP::PostUploadPack` and `SmartHTTP::PostReceivePack`](https://gitlab.com/gitlab-org/gitaly/issues/219) | #92 | gitlab-org/gitaly-proto!4 | #122 | #125 | #184 | | |
| [`SSH:SSHUploadPack` and `SSH::SSHReceivePack`](https://gitlab.com/gitlab-org/gitaly/issues/218) | #91 | gitlab-org/gitaly-proto!5 | #123 | #124 | | | |
| [`Ref::FindLocalBranches`](https://gitlab.com/gitlab-org/gitaly/issues/217) | #127 | #128 | !103 | gitlab-org/gitlab-ce!10059 | | | |
| [`Ref::count_branch_names` and `Ref::count_tag_names`](https://gitlab.com/gitlab-org/gitaly/issues/214) | #157 | -- | -- | gitlab-org/gitlab-ce!10780 | #215 | | |
| `Projects::GraphsController#charts` (shelved for now) | #158 |  | | | | | |
| [`Commit::GetAuthor`](https://gitlab.com/gitlab-org/gitaly/issues/216) | #159 | | | | | | |
| `Projects::RawController#show` | #160 | #195 | | | | | |
| [`CatFile::Blob`](https://gitlab.com/gitlab-org/gitaly/issues/212) | #194 |  | | | | | |

(More to follow!)

### Generating the Priorization Data

Use this script to generate a CSV of the 99th percentile accumulated for a 7 day period.

This data will change over time, so it's important to reprioritize from time-to-time.

```shell
(echo 'Controller,Amount,Mean,p99,p99Accum'; \
influx \
  -host performance.gitlab.net \
  -username gitlab \
  -password $GITLAB_INFLUXDB_PASSWORD \
  -database gitlab \
  -execute "SELECT sum(count) as Amount, mean(duration_mean) AS Mean, mean(duration_99th) AS p99, sum(count) * mean(duration_99th) as Accum FROM downsampled.rails_git_timings_per_action_per_day WHERE time > now() - 7d GROUP BY action" \
  -format csv | \
  grep -v 'name,tags,'| \
  cut -d, -f2,4,5,6,7| \
  sed 's/action=//' | \
  sort --general-numeric-sort --key=5 --field-separator=, --reverse \
) > data.csv
```

## Plan

We use our issues board for keeping our work in progress up to date in a single place. Please refer to it to see the current status of the project.

1. [Absorb gitlab_git](https://gitlab.com/gitlab-org/gitlab-ce/issues/24374)
1. [Milestone 0.1](https://gitlab.com/gitlab-org/gitaly/milestones/2)
1. [Move more functions in accordance with the iterate process, starting with the ones with have the highest impact.](https://gitlab.com/gitlab-org/gitaly/issues/13)
1. [Change the connection on the workers from a unix socket to an actual TCP socket to reach Gitaly](https://gitlab.com/gitlab-org/gitaly/issues/29)
1. [Build Gitaly fleet that will have the NFS mount points and will run Gitaly](https://gitlab.com/gitlab-org/gitaly/issues/28)
1. [Move to GitRPC model where GitLab is not accessing git directly but through Gitaly](https://gitlab.com/gitlab-org/gitaly/issues/30)
1. [Remove the git NFS mount points from the worker fleet](https://gitlab.com/gitlab-org/gitaly/issues/27)
1. [Remove gitlab git from Gitlab Rails](https://gitlab.com/gitlab-org/gitaly/issues/31)
1. [Move to active-active with Git Ketch, with this we can read from any node, greatly reducing the number of IOPS on the leader.](https://gitlab.com/gitlab-org/gitlab-ee/issues/1381)
1. [Move to the most performant and cost effective cloud](https://gitlab.com/gitlab-com/infrastructure/issues/934)


## Design

High-level architecture overview:

![Gitaly Architecture](https://docs.google.com/drawings/d/14-5NHGvsOVaAJZl2w7pIli8iDUqed2eIbvXdff5jneo/pub?w=2096&h=1536)

[Edit this diagram directly in Google Drawings](https://docs.google.com/drawings/d/14-5NHGvsOVaAJZl2w7pIli8iDUqed2eIbvXdff5jneo/edit)

## Presentations

- [Git Paris meetup, 2017-02-22](https://docs.google.com/presentation/d/19OZUalFMIDM8WujXrrIyCuVb_oVeaUzpb-UdGThOvAo/edit?usp=sharing) a high-level overview of what our plans are and where we are.
- [Gitaly Basics, 2017-05-01](https://docs.google.com/presentation/d/1cLslUbXVkniOaeJ-r3s5AYF0kQep8VeNfvs0XSGrpA0/edit#slide=id.g1c73db867d_0_0)
