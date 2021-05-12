# Virtual Storage

Praefect hides the distributed nature of the storage cluster from the client by exposing an interface that looks like a single storage. In Praefect, this abstraction is called a *virtual storage*. Each virtual storage has one or more *physical storages*, the Gitaly nodes, attached to it.

## Data Model

Praefect records the expected state of each repository within a virtual storage in the `repositories` table:

| virtual_storage | relative_path                                                                      | generation |
|-----------------|------------------------------------------------------------------------------------|------------|
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | 5          |

The `repositories` table has three columns: [^1]
1. `virtual_storage` indicates which virtual storage the repository belongs in.
1. `relative_path` indicates where the repository should be stored on a physical storage.
1. `generation` is monotonically increasing version number that is incremented on each mutator call to the repository.

`repository_assignments` table records which physical storages are supposed to contain a replica of a repository.

| virtual_storage | relative_path                                                                      | storage  |
|-----------------|------------------------------------------------------------------------------------|----------|
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | gitaly-1 |
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | gitaly-2 |

The number of assigned storages each repository has indicates its desired replication factor. Each record contains:

- The `(virtual_storage, relative_path)` tuple to uniquely identify a repository.
- The `storage` that is assigned to host a replica.

If there are no assignments for a repository, every physical storage is considered to be assigned.
previous behavior of replicating a repository on every physical storage.

Praefect tracks the current state of a repository on each physical storage in the `storage_repositories` table:

| virtual_storage | relative_path                                                                      | storage  | generation |
|-----------------|------------------------------------------------------------------------------------|----------|------------|
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | gitaly-1 | 5          |
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | gitaly-2 | 5          |

The `storage_repositories` table has four columns:
1. `virtual_storage` indicates which virtual storage the repository belongs in.
1. `relative_path` indicates where the repository should be stored on a physical storage.
1. `storage` indicates which physical storage this record belongs to.
1. `generation` indicates the minimum generation of the repository on this storage.

While similar to `storage_repositories`, the `repositories` table is needed to infer whether a repository was deleted or is waiting to be replicated to a physical storage. The records in `repositories` table additionally act as repository specific locks which should be acquired on updates to synchronizes access to `storage_repositories`.

## Generation Counters and their Increments and Propagation

Repository's version is tracked by a generation counter stored in `repositories` table. Generation counter is incremented on each successfully applied mutator operation. Each generation maps to a single mutator operation on the target repository. `storage_repositories` table tracks the minimum generation of a repository on each physical storage.

1. Without reference transactions, the primary applies the mutator. On success, Praefect atomically increments the repository's generation in the `repositories` table and sets the primary's record in `storage_repositories` to match. Secondaries are considered outdated as soon as the generation is incremented.
1. With reference transactions, healthy secondaries on the same generation as the primary participate in the transaction together with the primary. On transaction completion, Praefect atomically increments the generation counter in `repositories` and sets the primary's generation to match it in `storage_repositories`. Secondaries' generations are only incremented if they were on the same generation as the primary at the time of the increment. This is to avoid incrementing the generation number for secondaries that failed a concurrent transaction.

In both cases either all or some secondaries are left outdated. Praefect schedules replication jobs to each outdated secondary. When the replication job is successfully applied, the target repository's generation is set to match the source storage's generation was prior to starting the replication. When applying a replication job, Praefect first checks the source repository of the replication job is on a later generation than the target repository. This prevents replication jobs from downgrading a repository. Each replication might include later changes if the source repository was concurrently updated after the source repository's generation was checked. This is inconsistency is amended by a later replication job that is a no-op replication wise but sets the correct generation number. Given these properties, the generations guarantee that the repository has replicated up the changes that produced a given generation number but might also include later data.

**Note:** Praefect only enforces the downgrade protection if the target repository has a recorded generation. If the target or both source and the target do not have recorded generations, the replication job is allowed go through as Praefect does not know the state of the repositories. This behavior is allowed as a cluster prior to repository generations will not have a record for a given repository but might produce replication jobs. An upgraded cluster should never produce a replication job for a repository that does not have a generation record. This behavior can be disabled once migration is performed as described in [#3003](https://gitlab.com/gitlab-org/gitaly/-/issues/3033).

## Identifying Inconsistencies

Praefect identifies inconsistencies in the storage cluster by cross-referencing the expected state in the `repositories` and `repository_assignments` with the actual state of the physical storages in `storage_repositories`.

Expected state of physical storages can be attained by cross joining the configured physical storages with the expected repositories of the virtual storage in the `repositories` table. It's important to use configured storages as some physical storages might have been added to or removed from the virtual storage.

Possible inconsistencies and their reconciliations are listed below. Each of the scenarios assume a virtual storage called `default` with a primary storage `gitaly-1` and a secondary storage  `gitaly-2`.

### Missing Repository

Praefect expects an up to date copy of a repository to be present on every assigned physical storage. However, a physical storage might be missing a replica. This can be due to two reasons:

- A repository was just created. The primary `gitaly-1` contains the new repository but it has not yet been replicated to the secondary `gitaly-2`. This might be a temporary situation while the secondary is waiting to replicate the changes.
- A physical storage was assigned as a new host for the repository. The assignment has been recorded for `gitaly-2` but the repository has not yet been replicated to the storage.

`repositories`:

| virtual_storage | relative_path                                                                      | generation |
|-----------------|------------------------------------------------------------------------------------|------------|
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | 0          |

`repository_assignments`:

| virtual_storage | relative_path                                                                      | storage    |
|-----------------|------------------------------------------------------------------------------------|------------|
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | gitaly-1   |
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | gitaly-2   |

`storage_repositories`:

| virtual_storage | relative_path                                                                      | storage  | generation |
|-----------------|------------------------------------------------------------------------------------|----------|------------|
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | gitaly-1 | 0          |

To fix the inconsistency, reconciler schedules `update`-type jobs to the storages missing the repository from random healthy storages with up to date replicas.

### Outdated Repository

A repository is considered outdated if its `generation` number in `storage_repositories` does not match the expected generation in the `repositories` table. This might be due to two reasons:

1. The primary received a new mutator which was not yet replicated to the outdated physical storage.
2. Administrator accepted data loss in the repository. Accepting data loss increments the expected generation in `repositories` and sets the selected authoritative storage's generation to match in `storage_repositories`, leading every other copy of the repository to be considered outdated. See [Gitaly Cluster documentation](https://docs.gitlab.com/ee/administration/gitaly/praefect.html#accept-data-loss) for more information.

In the case below, `gitaly-2` has an outdated version of the repository as its generation does not match what's in the `repositories` table. `gitaly-2` is behind by three changes as generation counters starts from zero. If a physical storage is missing a repository, its generation should be considered to be `-1` to correctly calculate the number of changes it is behind.

`repositories`:

| virtual_storage | relative_path                                                                      | generation |
|-----------------|------------------------------------------------------------------------------------|------------|
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | 2          |

`repository_assignments`:

| virtual_storage | relative_path                                                                      | storage    |
|-----------------|------------------------------------------------------------------------------------|------------|
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | gitaly-1   |
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | gitaly-2   |

`storage_repositories`:

| virtual_storage | relative_path                                                                      | storage  | generation |
|-----------------|------------------------------------------------------------------------------------|----------|------------|
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | gitaly-1 | 2          |
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | gitaly-2 | 0          |

To fix the inconsistency, reconciler schedules `update`-type jobs to the storages missing the repository from random healthy storages with up to date replicas.

### Unexpected Repository

#### Deleted Repository

A physical storage might contain a repository that is not expected be present on the virtual storage. This can happen if a repository is deleted. When processing the deletion, the primary deletes its copy of the repository. On success, it deletes the repository's record in the `repositories` table and its own record in the `storage_repositories` table. Secondaries might still have a record in the `storage_repositories` while they are waiting to replicate the deletion operation.

**NOTE:** It is safe to delete repositories which have a record in `storage_repositories` but not in `repositories`. It is **not safe** to delete repositories which are present on the disk but not in either of the tables. These are repositories which have not received a mutator after the repository tables were implemented. Migrating every repository to these tables is tracked in [#3003](https://gitlab.com/gitlab-org/gitaly/-/issues/3033).

`repositories`:

| virtual_storage | relative_path                                                                      | generation |
|-----------------|------------------------------------------------------------------------------------|------------|

`repository_assignments`:

| virtual_storage | relative_path                                                                      | storage    |
|-----------------|------------------------------------------------------------------------------------|------------|

`storage_repositories`:

| virtual_storage | relative_path                                                                      | storage  | generation |
|-----------------|------------------------------------------------------------------------------------|----------|------------|
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | gitaly-2 | 2          |

Praefect's reconciler doesn't fix the inconsistency at this time. A fix is tracked in https://gitlab.com/gitlab-org/gitaly/-/issues/3480.

### Unassigned Replica

A physical storage might contain a replica of a repository even if it is not assigned to host it. This can happen if the storage was previously
assigned to host the repository but was later unassigned. Praefect doesn't keep unassigned copies up to date via transactions nor replication jobs.
Below, `gitaly-2` has been unassigned but still contains a replica of the repository.

`repositories`:

| virtual_storage | relative_path                                                                      | generation |
|-----------------|------------------------------------------------------------------------------------|------------|
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | 2          |

`repository_assignments`:

| virtual_storage | relative_path                                                                      | storage    |
|-----------------|------------------------------------------------------------------------------------|------------|
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | gitaly-1   |

`storage_repositories`:

| virtual_storage | relative_path                                                                      | storage  | generation |
|-----------------|------------------------------------------------------------------------------------|----------|------------|
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | gitaly-1 | 2          |
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | gitaly-2 | 2          |

### Removed Physical Storage

When a physical storage is removed from the virtual storage configuration, it leaves behind its records in the `storage_repositories` table. These records should be ignored. If the physical storage is connected back again, it will continue from its previous state. If the physical storage is never going to be attached to the virtual storage again, its records could be removed. Praefect doesn't have functionality to do this yet.

In the example below, assume that `gitaly-2` has been removed from the configuration. `gitaly-2` had the most up to date version of a repository in the virtual storage. The expected state of the virtual storage in `repositories` table still records the latest generation. Repositories that were not up to date with the removed physical storage would be considered outdated.

Assignments of unconfigured storages are ignored as well. This means a repository's replication factor decreases when an assigned storage is removed
from the virtual storage. Below, the repository's replication factor is `1` as `gitaly-2` has been removed from the configuration.

`repositories`:

| virtual_storage | relative_path                                                                      | generation |
|-----------------|------------------------------------------------------------------------------------|------------|
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | 3          |

`repository_assignments`:

| virtual_storage | relative_path                                                                      | storage    |
|-----------------|------------------------------------------------------------------------------------|------------|
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | gitaly-1   |
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | gitaly-2   |

`storage_repositories`:

| virtual_storage | relative_path                                                                      | storage  | generation |
|-----------------|------------------------------------------------------------------------------------|----------|------------|
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | gitaly-1 | 2          |
| default         | @hashed/5f/9c/5f9c4ab08cac7457e9111a30e4664920607ea2c115a1433d7be98e97e64244ca.git | gitaly-2 | 3          |

The reconciler considers assigned but removed storages as still assigned. This means it won't schedule `delete_replica` jobs to any assigned storage before the assignments
of the removed storages are manually removed.

## Known Problems

1. When a primary is demoted, it might be in process of accepting a write. If there is a concurrent write to the new primary, one of the writes is going to be lost as the primary increments its generation even if it was not on the latest one. This issues and the solution proposed is tracked in [#2969](https://gitlab.com/gitlab-org/gitaly/-/issues/2969).

1. Not all repositories have generation records in a cluster that was upgraded. This issue will be solved once an appropriate migration tool is implemented. The issue is tracked in [#3003](https://gitlab.com/gitlab-org/gitaly/-/issues/3033).

1. There are some mutator operations that increment the generation of a repository even though they do not mutate the references. This causes repositories to be considered outdated without a reason. This issue is tracked in [#2977](https://gitlab.com/gitlab-org/gitaly/-/issues/2977)

1. Mutators are applied to the primary and reference transaction participants prior to recording the work in anyway. If the mutator succeeds but incrementing the generation fails, Praefect will know about the inconsistent state. This issue is tracked in [#2960](https://gitlab.com/gitlab-org/gitaly/-/issues/2960).

## Footnotes

[^1]: A repository is uniquely identified by its primary key `(virtual_storage, relative_path)`. While Praefect doesn't expect a specific format for the relative path, it helps to know that it is generated in GitLab by hashing the unique ID of the GitLab project. To find out more, read about [hashed storage](https://docs.gitlab.com/ee/administration/repository_storage_types.html#hashed-storage).









