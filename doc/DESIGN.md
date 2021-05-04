## 	Reason

### Git Characteristics That Make Horizontal Scaling Difficult

Git's fundamental behaviors are similar to relational database engines and are difficult to horizontally scale for the same reasons that serverless database is challenging and why serverless database cannot handle all existing relational database workloads.

Gitaly is a layer that brings horizontal scaling and higher availability to massively scaled Git operations through a variety of optimizations in disk locality, caching results of intensive operations (like git pack-objects), coordinating between multiple nodes, cluster synchronization and sharding.

> **Note:** While Gitaly is designed to help Git scale horizontally, Gitaly internal operations depend on the standard open source release of the git client which it calls during git operations. So some Gitaly limitations still pass through from Git. The same is true of any server system that does not have a layer like Gitaly - but in such cases there is no ability to provide any horizontal scaling support at all.
#### Git Architectural Characteristics and Assumptions

- **Stateful, Atomic, ACID Transactions** (“database synonymous” workload with regard to memory / CPU / disk IO).
- **"Process Atomic" Transactions** - requires one commit to be coordinated by one and only one Git process.
- **Atomic Storage** - assumes that operations of a single git command write to a single storage end-point.
- **Storage channel speeds** - assumes low latency, high bandwidth storage access (near bus speeds).
- **ACID Isolation** - by design Git allows concurrent update access to the same repository as much as possible, in the area of updating Git Refs, record locking is necessary and implemented by Git.
- **Wide ranging burst memory / CPU / disk IO requirements** - assumes significant available memory headroom for operations that intensify depending on the content size.

#### Specific Git Workload Characteristics That Make Remote File Systems and Containerization of Gitaly Challenging

**IMPORTANT:** The above characteristics and assumptions combined with specific Git workloads create challenging compute characteristics - high burst CPU utilization, high burst memory utilization and high burst storage channel utilization. Bursts in these compute needs are based on Git usage patterns - how much content, how dense (e.g. binaries) and how often.

These workload characteristics are not fundamentally predictable across the portfolio of source code that a given GitLab server may need to store. Large monorepos might exist at companies with few employees.  Binaries storage - while not considered an ideal file type for Git file systems - is common in some industry segments or project types. This means that architecting a GitLab instance with built-in Git headroom limitations causes unexpected limitations of specific Git usage patterns of the people using the instance.

These are some of the most challenging git workloads for Git:
- Large scale, busy monorepos (commit volume is high and packs for full clones are very large).
- High commit volume on a single repository (commit volume is high packs for full clones are very frequent).
- Binaries stored in the Git object database. (In GitLab Git LFS can be redirected to PaaS storage).
- Full history cloning - due to packfile creation requirements.

The above workload factors compound together when a given workload has more than one characteristic.
#### Affects on Horizontal Compute Architecture
- The memory burstiness profile of Git makes it (and therefore Gitaly) very challenging to reliably containerize because container systems have very strong memory limits. Exceeding these limits causes significant operational instability and/or termination by the container running system.
- The disk IO burstiness profile of Git makes it (and therefore Gitaly) very challenging to use remote file systems with reliability and integrity (e.g.  NFS - including PaaS versions). This was, in fact, the first design reason for Gitaly - to avoid having the Git binary operate on remote storage.
- The CPU burstiness profile of Git (and therefore Gitaly) also makes it challenging to reliably containerize.

These are the challenges that imply an application layer is needed to help Git scale horizontally in any scaled implementation - not just GitLab.  GitLab has built this layer and continues to chip away (iterate) against all of the above challenges in this innovative layer.
### Evidence To Back Building a New Horizontal Layer to Scale Git
For GitLab.com the [git access is slow](https://gitlab.com/gitlab-com/infrastructure/issues/351).

When looking at `Rugged::Repository.new` performance data we can see that our P99 spikes up to 30 wall seconds, while the CPU time keeps in the realm of the 15 milliseconds. Pointing at filesystem access as the culprit.

![rugged.new timings](doc/img/rugged-new-timings.png)

Our P99 access time to just create a `Rugged::Repository` object, which is loading and processing the git objects from disk, spikes over 30 seconds, making it basically unusable. We also saw that just walking through the branches of gitlab-ce requires 2.4 wall seconds.

We considered to move to metal to fix our problems with higher performance hardware. But our users are using GitLab in the cloud so it should work great there. And this way the increased performance will benefit every GitLab user.

Gitaly will make our situation better in a few steps:

1. One central place to monitor operations
1. Performance improvements doing less and caching more
1. Move the git operations from the app to the file/git server with git rpc (routing git access over JSON HTTP calls)
1. Use Git ketch to allow active-active (push to a local server), and distributed read operations (read from a secondary). This is far in the future, we might also use a distributed key value store instead. See the [active-active issue](https://gitlab.com/gitlab-org/gitlab-ee/issues/1381). Until we are active active we can just use persistent storage on the cloud to shard, this eliminates the need for redundancy.

## Scope

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

## Decisions

All design decisions should be added here.

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
1. GitLab already has [logic so that the application servers know which file/git server contains what repository](https://docs.gitlab.com/ee/administration/repository_storages.html), this eliminates the need for a router.
1. Use [gRPC](http://www.grpc.io/) instead of HTTP+JSON. Not so much for performance reasons (Protobuf is faster than JSON) but because gRPC is an RPC framework. With HTTP+JSON we have to invent our own framework; with gRPC we get a set of conventions to work with. This will allow us to move faster once we have learned how to use gRPC.
1. All protocol definitions and auto-generated gRPC client code will be in the gitaly repo. We can include the client code from the rest of the application as a Ruby gem / Go package / client executable as needed. This will make cross-repo versioning easier.
1. Gitaly will expose high-level Git operations, not low-level Git object/ref storage lookups. Many interesting Git operations involve an unbounded number of Git object lookups. For example, the number of Git object lookups needed to generate a diff depends on the number of changed files and how deep those files are in the repository directory structure. It is not feasible to make each of those Git object lookups a remote procedure call.
1. By default all Go packages in the Gitaly repository use the `/internal` directory, unless we explicitly want to export something. The only exception is the `/cmd` directory for executables.
1. GitLab requests should use as few Gitaly gRPC calls as possible. This means it is OK to move GitLab application logic into Gitaly when it saves us gRPC round trips.
1. Defining new gRPC calls is cheap. It is better to define a new 'high level' gRPC call and save gRPC round trips than to chain / combine 'low level' gRPC calls.
1. Why is Gitaly written in Go? At the time the project started the only practical options were Ruby and Go. We expected to be able to handle more traffic with fewer resources if we used Go. Today (Q3 2019), part of Gitaly is written in Ruby. On the particular Gitaly server that hosts gitlab-org/gitlab-ce, we have a pool of gitaly-ruby processes using a total 20GB of RSS and handling 5 requests per second. The single Gitaly Go process on that machine uses less than 3GB of memory and handles 90 requests per second.