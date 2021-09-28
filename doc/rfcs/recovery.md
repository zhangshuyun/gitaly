# Recovery

Making Gitaly Cluster resilient to unexpected changes in the nodes
requires being able to detect and recover from faults. We outline the
requirements of such a recovery mechanism, the current status of Gitaly
Cluster, and how we move forward today to enable a more robust recovery
of out-of-sync nodes.

## Requirements

1. Gitaly Cluster should be able to detect and automatically recover
from a single repository going out of sync. Example failure scenarios:

    1. An admin restores from a backup snapshot.
    1. An admin deletes or modifies a repository.
    1. A hardware failure corrupts a repository.
    1. An admin attempts to rebuild a node by rsync'ing from existing repositories.

1. A new Gitaly node should be able to added and automatically rebuild
itself with all repositories that should be on that virtual storage.

## Current status

The [current implementation of virtual storages](../virtual_storage.md)
assumes that the Praefect database reflects the correct state of the
repositories of all Gitaly nodes. Each row in the `repositories` table
corresponds to a project in the Rails `projects` database, and each
Gitaly node stores the version of a repository in the `generation` column.

There are a number of issues with this approach:

1. The projects listed in Praefect database and the GitLab Rails
database must be kept in sync. For example, if a path belonging to
project 1000 is not listed in the Praefect database, there are a number of
failure modes:

    1. Praefect will not be able to route Gitaly requests for those
    projects. For example, visiting a project repository may result in a 404
    error.
    1. Praefect will not be able to replicate project 1000 since it
    does not know about it.

1. Gitaly nodes could be modified (e.g. restored from backup) without
updating the `generation` column. As a result, Praefect might direct a
read or write to a node that has gone out of sync with the cluster. We
have seen this happen when admins restore a snapshot from backup, or
when Geo renames project repositories.

    Suppose we have 3 nodes in the Gitaly Cluster: nodes A, B, and C.  There
are a number of failure modes that can result:

    1. Pushes to the repository may fail. Suppose nodes A and B have the
    highest `generation` number, say, 10. Node C is at `generation` 9. If
    someone quietly restores from an old snapshot in node B, there is no
    longer a quorum. Writes to node B will fail and cause the majority vote
    to fail until node C has caught up. Node B will not be resynched
    automatically even though it is behind.
    1. Praefect may stop directing reads to node B, causing more pressure on
    other nodes.
    1. A read from a repository could cause a 500 error when viewing
    a page (e.g. merge request, commit, etc.).
    1. A background job may fail trying to read Gitaly data.

## Making Gitaly Cluster more resilient

Gitaly Cluster needs to be able to detect and recover from the failures
above with almost no manual intervention. Here are a list of
recommendations:

### Use repository checksums

To make Gitaly Cluster more resilient, we should consider moving away
from using a static `generation` field that does not necessarily reflect
the actual state of the repository to using a field that can help detect
differences easily. The natural candidate would be the repository
checksum, which we already use with Geo, that takes the SHA values from
a subset of the references (e.g. `refs/heads/*`, `refs/tags/*`, etc.)
and XORs each value together. This has the nice property that
dynamically updating the checksum is a matter of XOR'ing the old value
and XOR'ing the new value.
