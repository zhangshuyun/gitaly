# Design notes for Git object deduplication

## Problem description

Forking projects is a common workflow in GitLab. When a user forks a
project, GitLab creates a full clone of the repository associated with
the fork parent. If the repository is large and gets forked often, this
leads to a lot duplicated Git objects which take up disk space.

We are adding Git object deduplication to GitLab to address this
problem.

## Solution overview

We have [chosen](https://gitlab.com/gitlab-org/gitaly/issues/1331) a
design where Git objects shared between repositories on the same Gitaly
storage shard can be shared via a **pool repository**. For each member
of the pool, the pool repository has a git remote pointing to it.
Conversly, each member points to the pool repository as an **alternate
object directory** using the `objects/info/alternates` in the
repository.

From a Gitaly point of view this is a very transparent solution. Almost
all RPC's should continue to work without modification on repositories
that are linked to a pool.

What is new is that GitLab must manage the pool repository and the pool
relations in Gitaly.

## Limitations

-   Pools are local to Gitaly storage shards.
-   Repositories in a pool can see all objects in the pool repository if
    they know the object ID (SHA1).

The second property means that we should not mix repositories of
projects with different visibility scope (e.g. public vs private) in the
same storage pool.

This also means that we cannot conflate the project fork network
relation with the repository pool relation. Storage pools will be
restricted within fork networks to public projects that live on the same
Gitaly shard.

If a project in a storage pool changes visibility from public to private
we must extract it from the storage pool.

## First iteration

The first iteration of object deduplication is limited to the following scope:

-   Only works with new forked projects
-   The parent of the forked project is using hashed storage
-   The new forked project is using hashed storage
-   Pool repositories are created once and do not pull in new git
    objects. This means that the deduplication percentage will fall over
    time as new objects get pushed to the repositories in a pool. We
    will address this in a later iteration ("pool grooming")

### Scenarios

#### Create pool from existing repo

- SQL: create pool object
- Gitaly: create pool repo from existing repo. Create remote pointing to existing repo, and clear top level refs in pool.
- SQL: link project to pool
- Gitaly: finalize link: create (remote and) objects/info/alternates connection for existing repo

If this fails in the middle there is no data loss in the existing repo.

#### Clone new repo from origin in pool (e.g. a fork)

- SQL: create project linked to pool. Project is in "being cloned" state
- Gitaly: create new repo with local disk clone from origin
- Gitaly: create remote and objects/info/alternates connection for new repo
- SQL: clear project "being cloned" state

#### Project leaves pool (e.g. fork taken private)

- SQL: mark project as "repo transitioning to private". git pushes are blocked
- Gitaly: copy needed objects from pool with git repack -a
- Gitaly: remove objects/info/alternates link and pool remote
- SQL: unmark "repo transitioning to private". git pushes no longer blocked

This is problematic. If we fail in the middle, git pushes remain
blocked. Do we really need to block git pushes during this operation?
If we fail during the Gitaly parts we can re-create the pool links and restart the repack.