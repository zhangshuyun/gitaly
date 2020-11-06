# RFC: Resource limits through control groups

## Problem statement

Gitaly is designed and operated with a large number of repositories co-located
on the same system. Gitaly has no control over the number of repositories it
hosts, and little control over the number of request to be handled concurrently.
These properties lead to competition for resources, and Gitaly currently relies
on the operating system to manage these competing interests.

While Gitaly, and Git, manage to perform well within the constraints of their
host system, there's been a number of incidents where a limited subset of
requests obtained the vast majority of resources requiring the operating system
to free up resources. Meanwhile a degraded service is experienced for end-users.

When a Gitaly host runs out of memory, the operating system will first use swap,
then free file mapped resources, but ultimately it will have to use the OOM Killer.
More or less randomly killing processes randomly until enough memory is
available again. Gitaly does not control this process, and cannot hint to the
OOM Killer which processes might be worth killing.

These situations are most common when `git-upload-pack(1)` invokes
`git-pack-objects(1)` due to a client executing `git-receive-pack(1)`, that is
a fetch or clone. While GitLab and Gitaly try to be prepared for these requests,
there's both pathological cases as well as inherent costs to such requests it's
straining resources, and degrading performance for all concurrent requests
handled by Gitaly. Also known as the [noisy neighbour][wiki-neighbour] problem.

For this document the scope is limited to memory+swap usage, as well as CPU
utilization. On what entity to limits are to be applied will later discussed.

[wiki-neighbour]: https://en.wikipedia.org/wiki/Cloud_computing_issues#Performance_interference_and_noisy_neighbors

## Potential solution: control groups

Linux kernels since 2.6 expose an API commonly referred to as
[cgroups][cgroup-man7]. It allows resource distribution by creating a grouping
and adding resource constraints to this group, for example a limit share of CPU
capacity, and a maximum of available memory and/or swap. To this group processes
are added, to which from that point on the limits apply. When a process spawns
child processes, these limits are inherited. 

Currently GitLab.com executes Gitaly, and adds it to its own cgroup, ensuring
that at no point in time all resources are consumed and auxiliary processes
continue to run. This didn't require any application changes, and is the choice
of an operator. This RFC is to expand on these with sub-control groups.
Subgroups again inherit the limits of their parent group, though are interesting
as they can be used to divide the limits down further to newly created groups.
There's a need for application changes to create subgroups and maintain them, 
so concurrent requests that are assigned to the correct subgroup. Essentially
requiring the application to create, maintain, and delete buckets, and analyzing
incoming requests and assigning them to these buckets. To limit the scope of the
first iterations of changes and this document, maintaining subgroups by varying
their resource allocation at runtime is considered to problem for another day.

The most prominent entity that can be assigned to subgroups are processes. Given
`git(1)` is the main method to query a repository, and `git-upload-pack(1)` the
main culprit of high memory and CPU consumption these should be constraint to
prevent virtually unbounded hoarding of resources.

[cgroup-man7]: https://www.man7.org/linux/man-pages/man7/cgroups.7.html

### Assigning sub groups

Creating and deleting subgroups are cheap, and while there's obviously always
limits to the amount of subgroups one could create, these are so high that for
the sake of discussion these are considered unlimited.

The goal of these subgroups if to create an guaranteed upper-bound for each
subgroup for it's obtainable resources, which in turn should create an safe
expectation of resources for it's peers, and fairness.

#### Per process

One naive solution would be to put each process Gitaly spawns into their own
subgroup. A group get created when executing a sub process, and each sub group
has the same limits. Let's consider this grouping for both memory and CPU.

In the case of a limitation on a maximum of allocated bytes this strategy is
viable. Even when the total of maximum allocatable bytes of each subgroup
exceeds the parent group maximum of allocatable bytes, this works in cases where
the parents limit vastly exceeds each subgroups limit. When the parent group limit
is exceeded, out of memory events will be send to subgroups, much like the
OOM Killer works. The key difference, is that each process had the
same upper bound of memory, and in this situation it's not one process that
hoards the vast majority of resources. Decreasing the impact of
the problem this RFC aims to resolve.

Considering the CPU, per process subgroups will likely have the adverse effect.
CPU limitations are based on the notion that one doesn't have a maximum of
operations or time but the CPU has shares, that is; a relative amount of 
available CPU. CPU has to be divided based on shares as the resource has
different capacity throughout its operating time as opposed to memory. Consider
the case where after peak load the CPU will be automatically down-tuned to
reduce the energy consumed. And while it's possible to create new shares of CPU,
new shares would dilute the current pool and as such not resolve the noisy
neighbour problem. While it could be argued that dilution will always happen
during peak load, the impact should be contained, which this solution now
doesn't provide. 

#### Per repository

Nearly all operations Gitaly does, are scoped to repositories. And while Gitaly
itself doesn't know what repositories are currently being stored without crawling
the `git-data` directory, it's told in each RPC what the path is of each
repository. On top of that, RPCs are annotated if these are
[repository scoped RPCs][../../proto/lint.proto].

Per repository subgroups still run into issues described in per process
subgrouping. But given N processes are serving R repositories, it holds that
R <= N. So the effect is dampened. To extend this, for larger installations
in particular, the number of users that can and will interact with a repository
varies per repository. That means that repositories can act as a heuristic for
users.

The caveat is that some RPCs operate on multiple repositories, in which case a
repository of the these has to be chosen to determine the subgroup.

Note also that subgroups other than per process subgrouping incur a slightly
higher cost in complexity, as well as runtime costs, due to the fact one can
only delete a subgroup if there's no (zombie) processes member of the group.

#### Per user

Like repositories, users are unknown to Gitaly. But unlike repositories, there's
currently no fast lookup possible to determine who the end user is for each RPC
request. A client could provide this information, but usually it won't as
there's currently no need for it, as Gitaly doesn't handle either authentication
or authorization. Requiring this data would involve a lot of changes to Gitaly,
and have cascading changes for each of Gitaly's clients. Futhermore, there's
operations that aren't triggered by users, but by GitLab. Considering these two
arguments, user based subgroups have limited viability without a much broader
redesign of GitLab.

### Subgroup resource allocation

TODO: Currently thinking about a first iteration where each subgroup gets equal
shares CPU and an equal hard memory maximum. Limiting initial complexity and
allowing for iterations and experiments later.

## Benefits of control groups

Cgroups exist since Linux kernel versions 2.6.24, meaning it's well over 10 years
old. While in those 10 years there's been additions and iterations, it's
considered mature and boring technology. There's widespread industry adoption and
support in all major Linux distributions, usually through `systemd`. Cgroups
perform well in a large number of companies, and allow for example cloud
providers to proof they meet SLAs.

Additionally, using kernel managed resource management removes the need for
complexity in the Gitaly code base, or at least minimizes it. Than it could be
argued that the kernel will always be a natural fit for resource management,
while it's hard to argue that Gitaly should reimplement logic already provided.

While this doesn't resolve a class of bugs, it might be fair to state a class of
user facing issues are resolved with one feature.

## Risks and downsides

### Linux only

`CGROUPS(7)` are available on Linux kernels only. After 2020-11-22 all supported
Linux distributions will have V1 support for cgroups. GitLab officially only
supports Linux distributions, though the application is known to also run on
MacOS (many GitLab team members run MacOS), as well as FreeBSD.

These platforms currently enjoy an near on-par experience, while supporting
`cgroups` will create first and second class experiences. This split is created
throughout the Gitaly team. Currently that means that half of the Gitaly team
members cannot contribute through the GDK, but need additional tooling.

### Tight coupling with distribution methods

To create a `cgroup` elevated privileges are needed, than to manage it, the
cgroup needs to be owned by the same user that gitaly runs under, usually `git`.
This can best be achieved at the time when packages are installed, thus
requiring more coupling between gitaly and for example Omnibus-GitLab.

### V2 roll out

The cgroup API previously discussed is the V1 API, but a new API is implemented
too, V2. Currently all major platforms only support V1 without administrator
changes. The two versions have different interfaces, and are incompatible, hence
the major version bump. This might create a situation where some platforms
default to v2 in the future and Gitaly needs to add support for it, while
maintaining v1 support.

### Gitaly bugs might impact users more

When new behaviours are rolled out with increased memory consumption there's no
effect on users, as the nodes have plenty resources for day-to-day operations.
In effect buying time to remove the overhead which is now absorbed by over
provisioning. It's equally fair to flip this on it's head to reason bugs are 
sometimes not found as these are absorbed by over-provisioning.

However, with cgroups it could create a process that now runs into a memory
limit and an OOM event is triggered, meaning the user action won't be completed.

## Alternatives

### Better GitLab wide rate limiting

Gitaly is by no means the front door for clients. Each request is first handled
by either gitlab-workhorse, or gitlab-shell. These provide opportunities to rate
limit with higher granularity than Gitaly has. While an option, it solves only
part of what cgroups could solve, and would be orthogonal to cgroups.

### Improve Git/LibGit2

Using and tuning the usage of Git is what Gitaly developers are trained to do,
and are comfortable with. Making Git perform better for Gitaly administrators as
well as clients is being pursued regardless of the adoption of cgroups. However
this will never provide fine-grained over resource usage, and resolving
issues will mostly be a reactive action, while there's an interest in prevention
of the noisy neighbours to meet a service level as expected by GitLab users.

### Out of scope

The Ruby sidecar for Gitaly, Gitaly-Ruby, has soft limits applied to the workers,
usually around 300MB. These workers are managed by a
[supervisor](https://gitlab.com/gitlab-org/gitaly/-/blob/f4922bb91de88ffb476a06bba15b7828f25fe127/internal/supervisor/supervisor.go), which can remove workers from the load balancer
if these are consuming too much memory. Than each process will be killed after
60 seconds to reclaim the memory, and allow the currently handled requests to
finish.

Each of these workers could be added to their own subgroup to limit their memory
too. Though there's considerable investments being made 
