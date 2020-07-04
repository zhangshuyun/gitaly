# RFC: Praefect Dataloss and Recovery Strategy

## Abstract

This RFC proposes a strategy for Praefect to prevent, detect, and recover from dataloss.

## Dataloss Prevention

To reduce potential dataloss and conflicts, Praefect puts repositories into read-only mode when a primary-failover occurs.

A primary-failover happens when a primary node is no longer considered healthy by a Praefect cluster. The Praefect cluster will then designate a new Primary from one of the remaining nodes in order to continue serving requests.

Once a new primary has been designated, a repository is in one of the following possible modes:

- **Read-Write mode**
	- Happens when the new primary replica has the latest changes
	- Praefect will schedule new replications from the primary to any replicas missing the latest changes.
	- Praefect will continue to process both mutator and accessor RPCs for the repository
- **Recovery mode**
	- Happens when the new primary replica is missing the latest changes but the latest changes exist on another replica
	- Praefect will only process accessor RPCs until the primary receives the latest changes
	- Praefect will attempt to propagate changes from repos with the latest changes to the repos missing them
	- Once the primary is repaired, Praefect will start processing mutator RPCs again. This will put the repository into read-write mode.
- **Read-only mode**
  - Happens when none of the remaining replicas have the latest changes
  - Praefect will only process accessor RPCs
  - Read-only mode can be overridden manually by an administrator
  - Read-only mode can be resolved when a node containing the latest changes becomes healthy again. This will put the repository into recovery mode.

## Stateless Strategy

In the stateless design, the approach for determining outdated repositories gleans the information from the replication queue. We check for two things:

1. The latest replication job must be in `completed` state. Other states indicate possible unreplicated writes.

2. The source node must be the last writable primary. This is to avoid having to recursively check whether the source node contained the latest writes. The last writable primary would contain the latest writes.

The current approach is simple but has problems with over reporting data loss and not being flexible enough to support more advanced use cases.

From the points listed below, we can conclude that it is not sufficient to only look at the latest replication job and we can't expect the latest replication job to always come from the last writable primary when recovering from a failover. We need an alternative approach for more accurate information and to give us flexibility in repairing out of date repositories.

### Reporting Data Loss

1. Due to requiring the latest replication job's source node to be the primary, a node is still considered outdated when it is brought up to date from a secondary. This is problematic as:
   1. Replicating from a secondary may be necessary after a failover event. While Praefect elects the most up to date node as the new primary, the node might still be missing some writes. The newly elected primary might be missing writes to repository A where as the remaining secondaries were missing writes to repositories B, C and D. If the previous primary stays down, the only way to get the complete data on the new primary is by replicating repository A from one of the secondaries.
   1. `reconcile` only produces a replication job if the hashes of the source and the target repositories' references differ. If a repository is brought up to date from a secondary, the replication job not originating from the previous writable primary causes `dataloss` to report the repository as having possible data loss. However, since the repository is actually up to date, it is not possible to use reconcile to schedule another job from the previous writable primary again. This makes the data loss state unresolvable for the admin, even though the repository is actually up to date.

1. A repository is reported outdated if a second failover event happens without the repository receiving any writes. Example scenario could be:
   1. Primary node `gitaly-1` received a write and the resulting replication jobs to secondaries get successfully executed, including job `(gitaly-1 -> gitaly-3)`.
   1. `gitaly-1` goes down and stays down. `gitaly-2` is promoted as the new primary and write-enabled.
   3. `gitaly-2` goes down before receiving any writes and `gitaly-3` gets elected.
   4. Latest replication job to `gitaly-3` is from `gitaly-1`. `gitaly-2` is the previous writable primary, thus we incorrectly consider `gitaly-3` to be out of date.

The over reporting of data loss can cause a lot of confusion for an administrator. Reporting nodes outdated when they were brought up to date from a secondary prevents us from using secondaries for recovery after a failover as doing so would result the nodes being shown outdated in the data loss report, causing confusion. Not being certain whether a node was brought up to date also prevents us from automatically enabling writes again.

### Protecting Against Conflicting Writes in a Data Loss Scenario

Praefect switches a virtual storage in to read-only mode after a failover. This happens regardless of whether the failover caused data loss or not. Read-only mode is implemented virtual storage wide rather than per repository, which is unnecessarily wide in scope.

### Repairing Outdated Repositories

Reconcile works on a virtual storage which makes it difficult to repair data loss on the primary node after a failover. Primary node might be have outdated version of repository A, which is up to date on a secondary node. However, if the secondary node has any other outdated repositories, these would also be replicated to the new primary. While fixing this in reconcile is possible, Praefect should fix outdated repositories automatically. Repositories might get outdated during normal operation as well due to a failed replication job, not necessarily only after a failover.

### Acknowledging Data Loss

As read-only mode is implemented virtual storage wide, so is acknowledging data loss via the `enable-writes` subcommand. It is not possible to accept data loss for a given repository without accepting it for every repository.

## Proposed Solution

### Determining Repository Versions

By storing a version identifier in the database for each repository, we can easily determine whether the repository is up to date or not. To store the data, we need an additional table like below:

```sql 
CREATE TABLE repository_versions (
    virtual_storage TEXT,
    storage TEXT,
    relative_path TEXT,
    version BIGINT, 
    invalidated BOOLEAN,
    PRIMARY KEY (virtual_storage, storage, relative_path)
)
```

With each write to the primary, we increment the `version` column. This gives us a unique identifier for each write. We then produce replication jobs to the secondaries and include the version number of the write in the job. When the replication job was successfully applied, we update the target node's entry in the table to match the version stored in the replication job. This gives us a guarantee that the repository is at least on the version listed in the table. The repository might actually be of a later version in case there was a new write and the earlier write's replication job fetches the changes of the new write as well. This inconsistency is corrected when the replication job of the later write is applied, possibly not pulling in any changes but simply updating the version number in the table to match the version included in the job.

During replication, it could be that the repository fetch succeeds but updating the row in the table for the repository fails. We would then be serving two different versions of the repositories under the same version number. To prevent this, before fetching the repository as part of the replication, we should invalidate the target repository's entry in the table. The entry will be later added back when the replication job has successfully completed. If the repository's entry is invalidated in the table, we don't know the version of the repository and consider it outdated.

To summarize, each write coming in produces a new version where as each replication job propagates the version from the source node. By querying the table, we get the version information of a repository in a very easy manner.

### Reporting Data Loss

Data loss report is collected by querying the table for repository versions. Outdated repositories are defined as any repository which is not on the latest version or for which the version record is invalidated. If a repository is outdated, the report lists the latest version and the node that has it and the diverging nodes with their respective versions.

### Protecting Against Conflicting Writes in a Data Loss Scenario

If the repository on the primary node is not on the latest version, a failover has occured and the repository is outdated on the elected primary. Any new writes to the repository might conflict with the latest version and make data recovery efforts more difficult. To protect against this, Praefect rejects proxied mutator RPCs when the primary is not on the latest version of the target repository. When the repository is in read-only mode, replication jobs against the primary node are allowed to go through in order to support recovering data from a secondary node with a later version.

### Automatically Repairing Outdated Repositories

If a replication job triggered by a new write is not successfully completed, the target repository stays out of date until another replication job is triggered and successfully applied. This might be a long time if a repository is rarely written to. During this time window, the outdated repository can't be used for read distribution and there is an increased chance of data loss from a failover due to lower replication factor.

To address this, Praefect has a goroutine polling the version table for outdated repositories. If a repository is outdated, a replication job is scheduled to fix it if no replication jobs to the target repository are scheduled. This is due to the eventually consistent nature of replication. A repository might be outdated temporarily until the replication of a new write gets applied. This also prevents multiple Praefect nodes for scheduling multiple replication jobs to bring the outdated node up to date.

The source node of the replication job can be any available node with the most recent available version of the repository. This is particularly important when the repository is in read-only mode after a failover, where the most up to date node might be a secondary which does not have the very latest version of the repository. The virtual storage will heal to the best possible state and keeps improving as nodes with missing data come back online.

### Read Distribution

In normal operation, read operations can be distributed to the primary and up to date secondaries. When the repository is in read-only mode, the primary node is outdated. Until the repository on the primary node is brought up to date or the data loss is acknowledged, the reads are served from the secondaries with the most recent version. This avoids serving stale data from a newly elected primary.

### Acknowledging Data Loss

If the nodes that hold the latest version of a repository are gone for good, the administrator must acknowledge the data loss in order to enable the repository for writes again. The new writes produce replication jobs which overwrite any diverging data on the secondaries and can cause conflicts when attempting to recover data later. This is a manual step due to the potentially destructive nature.

When data loss is acknowledged, Praefect considers the version on the current primary as the authoritative version. Any records referring to later versions of the repository are invalidated, making the current version on the primary the latest, causing it to start accept writes.

### Migration Considerations

Repositories which do not have any entry in the table can't be accounted for as we do not have information on them. As long as a repository has an entry for at least one node in the table, the other copies of the repository can be reported outdated, read-only mode can be enforced if necessary and automatic repair can handle replicating to the outdated nodes.

Repositories get added to the table as they receive writes. However, as some repositories won't receive writes nor reads on a regular basis, we should consider a migration tool that walks the primary's storage and adds an initial version entry for each repository. Automatic repair process can then pick up the repos and schedule replication jobs to the secondaries introducing the missing entries in the process.

## Future with Reference Transactions

Replication jobs are not produced when reference transaction are in use. Versioning wise, major difference is that reference transactions might update only a subset of references where as replication jobs always fetch all of the references. Updating a simple version number is not sufficient anymore. While the transaction succeeding guarantees[^1] the updated references match on each node, other references might still be out of sync. As such, the version needs to represent the whole state of the repository. A hash of references can be used for this in a similar manner as is done in `reconcile`. We'll need to get the hash of references from the Gitaly nodes back to Praefect in some way. This could likely be achieved in a fairly transparent manner by implementing an interceptor in Gitaly that hashes the references of a repository after each mutator call. Praefect would then read the returned header and update the database with the returned hash.

Ideally we'd still have a way to determine which node has the most up to date data. With replication jobs, higher version number indicates the repository has the most up to date data. With each node being versioned with a non-linear hash, figuring out which node has most of the data becomes more difficult. Possible solution to this could be to keep using incrementing generation number along with a hash of references. The generation number would be incremented on each write as done in the eventually consistent approach. If a node fails to commit a transaction, it would be taken out of service until it is brought up to date with a replication job.

As references transactions are not production ready yet and supporting them increases the complexity of the approach, I propose we continue with the simple versioning scheme and migrate to an approach suitable for them later when it becomes necessary.

[^1]: This is the case as of 2020-07-01. There is ongoing work to allow transactions to commit without every Gitaly node agreeing to it.