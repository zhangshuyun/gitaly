# Pack-objects cache design notes

The purpose of this document is to give more insight into the design choices we made when building the first iteration of the pack-objects cache in https://gitlab.com/groups/gitlab-com/gl-infra/-/epics/372.

## Introduction

Please read the [administrator documentation for the pack-objects cache](https://docs.gitlab.com/ee/administration/gitaly/configure_gitaly.html#pack-objects-cache) if you are not already familiar with what it does or what it is for.

Please read [Pack-objects cache for CI Git clones epic](https://gitlab.com/groups/gitlab-com/gl-infra/-/epics/372) for more information about Pack-objects cache.

## High-level architecture

```
Gitaly (PostUploadPack)  git-upload-pack  gitaly-hooks  Gitaly (PackObjectsHook)  git-pack-objects
------------+----------  -------+-------  -----+------  -----------+------------  -------+--------
            |  fetch request    |              |                   |                     |
            +------------------>| pack request |                   |                     |
            |                   +------------->|   gRPC request    |                     |
            |                   |              +------------------>| pack request (miss) |
            |                   |              |                   +-------------------->|
            |                   |              |                   |                     |
            |                   |              |                   |<--------------------+
            |                   |              |<------------------+   packfile data     |
            |                   |<-------------+   gRPC response   |                     |
            |<------------------+ packfile data|                   |                     |
            |   fetch response  |              |                   |                     |
            |                   |              |                   |                     |
------------+----------  -------+-------  -----+------  -----------+------------  -------+--------
Gitaly (PostUploadPack)  git-upload-pack  gitaly-hooks  Gitaly (PackObjectsHook)  git-pack-objects
```

The whole pack-objects cache path depends on
[uploadpack.packObjectsHook](https://git-scm.com/docs/git-config#Documentation/git-config.txt-uploadpackpackObjectsHook)
option. When upload-pack would run git pack-objects to create a packfile for a
client, it will run `gitaly-hooks` binary instead. The arguments when calling
`gitaly-hooks` includes `git pack-objects` at the beginning. This pattern is
similar to how Gitaly handles Git hooks during a push (such as `pre-preceive`
and `post-receive`).

## Problem scope

We designed this cache to solve a specific problem on GitLab.com: high
Gitaly server CPU load due to massively parallel CI fetches.

That means:

1. It was OK if some types of fetch traffic did not become faster, as long as they also did not get slower
1. It was OK to make specific assumptions about the infrastructure this runs on

Example for (1): we made sure the cache can stream unfinished responses because without that, cache misses would be noticeably slower.

Example for (2): GitLab.com uses 16TB filesystems with at least 2TB of free space to store repositories. If our cache files are on there, and as long as the average size is reasonable, we don't have to worry about peak cache size. The worst case average cache size we projected was 30GB, which is just 1.5% of the 2TB of expected free space.

This is not to say that we think it is "bad" to want to use this cache for different types of traffic, or on different infrastructure, but it just wasn't our goal when we built it.

## Streaming and backpressure

One of the main goals was to have both **streaming** and
**backpressure**. By "streaming" we mean that a consumer of a cache
entry can start reading and stream data before the producer is done
writing the entry. We wanted this because Git fetches are relatively
slow and we did not want to add to the end-to-end latency. By
"backpressure" we mean that the producer of a cache entry blocks if
the consumers are not reading what the producer wrote. This has two
benefits. First of all, if the consumer hangs up, the producer gets a
write error and also stops. This way we don't write data to disk that
no-one will look at. Second of all, we get a natural limit on how much
IO bandwidth we use. On its own, the producer can write data faster
than the consumers can read it. If there was no backpressure, the
producer would put excessive write IO pressure on the cache storage
disk.

Because of streaming and backpressure, the producer and the consumers
of each cache entry must communicate with each other. By keeping the
cache local to a single Gitaly process, we made this a concurrent
programming problem, instead of a distributed systems problem.

The data structure responsible for streaming and backpressure is
`internal/streamcache.pipe`.

## Storage considerations

Early on in the project we thought we would use object storage to
store the cache data but we later decided not to, for several reasons.

1. Object storage would introduce a new point of failure to Gitaly
1. Local storage gets a boost from the operating system's page cache; there is no page cache for object storage
1. Local storage is simpler to work with

To support the second point: during testing on a server with high
cache throughput, where the number of bytes read from the cache peaks
over 100MB/s once an hour, we see disk reads at near 0 bytes per
second, with peaks of less than 1MB. This is possible because the
operating system is able to serve all the pack-objects cache reads from
the page cache, i.e. from RAM. This server does use a lot of IO
bandwidth, but that is all due to cache writes, not reads. With object
storage we would not get this extra layer of RAM caching from the
operating system.

Local files get a speed boost from RAM, and GitLab.com servers have lots of unused RAM.

## Off by default

The pack-objects cache is off by default because in some cases it
significantly increases the number of bytes written to disk. For more
information, see this issue where [we turned on the cache for
gitlab-com/www-gitlab-com](https://gitlab.com/gitlab-com/gl-infra/production/-/issues/4010#note_534564684).

It would be better if the cache was on by default. But, if you don't have
CI-like traffic, there is probably no benefit, and if your Gitaly
server can just manage its normal workload, the extra disk writes may push
it into saturation.
