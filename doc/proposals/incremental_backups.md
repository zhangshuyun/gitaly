# Incremental Backups

## Goal

Save time and money with automated incremental backups where a system admin can configure their GitLab
instance to make backups on a schedule, where each backup operation is an incremental backup. A restore would
be applying each incremental backup on top of each other.

## Design

### Making Backups

1. In this diagram, Rails first calls Gitaly to make a full backup. This will generate a full [Gitaly Bundle file](snapshot-storage.md).

2. Gitaly will send this file off to the object storage via a signed url. Once it gets a 200 OK, Gitaly will write the refs part of the
Gitaly Bundle file to a a file in the `.gitaly_bundle` directory in the repository directory with the checksum of the contents as the filename.

2. This file will be used to generate incremental backups. Gitaly will then take the checksum of
this refs snapshot and send it back to Rails. Rails will save this checksum in the database.

3. Next, Rails makes a request to Gitaly to generate an incremental snapshot. This time it provides the latest refs snapshot checksum it has.

4. When Gitaly receives this request, it looks in its `path/to/repo.git/.gitaly_bundle` for the refs snapshot. If it exists, then we know that
we can safely generate an incremental backup and send it to object storage. If it doesn't, then that means we can't and we fail the request. In
this failure mode, Rails would ask Gitaly to generate a full backup.

```mermaid
sequenceDiagram
  Rails->>Gitaly: create_full_backup(repo, "https://object_storage?id=full_backup_1")
  Gitaly->>Object Storage: "https://object_storage?id=full_backup_1"
  Object Storage->>Gitaly: HTTP 200 OK
  Gitaly->>Rails: HTTP 200 OK {"ref_checksum": "55f08600ea872b8e9ca7de5469d71a8711ba2aa2"}
  Rails->>Gitaly: create_incr_backup(repo, "https://object_storage?id=incr_backup_1" , "55f08600ea872b8e9ca7de5469d71a8711ba2aa2")
  Gitaly->>Object Storage: "https://object_storage?id=incr_backup_1",
  Object Storage->>Gitaly: HTTP 200 OK
  Gitaly->>Rails: HTTP 200 OK {"ref_checksum": "12b023b9b3872b8e3cfe2e7469d71a8711ba2d13"}
  Rails->>Gitaly: create_incr_backup(repo, "https://object_storage?id=incr_backup_2" , "12b023b9b3872b8e3cfe2e7469d71a8711ba2d13")
  Gitaly->>Object Storage: "https://object_storage?id=incr_backup_2",
  Object Storage->>Gitaly: HTTP 200 OK
  Gitaly->>Rails: HTTP 200 OK {"ref_checksum": "12b023b9b3872b8e3cfe2e7469d71a8711ba2d13"}
```

### Database Schema

We will store a record of the backups in the Rails Database while the actual bundle files will be shipped to
object storage. That way, we will have a linked list of incremental backups. This will also allow us to do compaction.

```mermaid
graph TD
  R[Rails] --> |1. What was the latest backup created?|D[Rails Database]
  D --> |2. Incremental backup with object_id 123|R
	R --> |3. Create an incremental backup with object_id 124 and send to S3| G[Gitaly]
	G --> |4. Store this backup bundle | O{S3}
  O --> |5. HTTP 200 OK| G
  G --> |6. I successfully created and sent bundle to S3| R
```

### Restoring from Backups

To restore a repository from backups, Rails will send repeated requests with signed requests containing the next incremental backup.
These will get applied to the repository using the method described in [Gitaly Bundle file](snapshot-storage.md).

```mermaid
sequenceDiagram
  Rails->>Gitaly: restore_backup(repo, "https://object_storage?id=full_backup_1")
  Gitaly->>Object Storage: retrieve bundle file
  Object Storage->>Gitaly: HTTP 200
  Gitaly->>Rails: OK
```

### Compaction
Incremental backups will begin to pile up. Restoring from many backups can be slow and expensive. We can optimize this process
by compacting the incremental backups into self-contained backups every once in a while.

```mermaid
sequenceDiagram
  Rails->>Database: what's the last full backup of repo1?
  Database->>Rails: {"id": 1, "repo_id": 1, "url": "https://<object_storage_url>?id=repo_1_full_backup_1", "parent_id": nil}
  Rails->>Database: what's the next child backup?
  Database->>Rails: {"id": 2, "repo_id": 1, "url": "https://<object_storage_url>?id=repo_2_incr_backup_1", "parent_id": 1}
  Rails->>Gitaly: compact_backups(full_backup_url="https://<object_storage_url>?id=repo_1_full_backup_1", incr_backup_url="https://<object_storage_url>?id=repo_2_incr_backup_1", destination_url="https://<object_storage_url>?id=repo_2_full_backup_2")
  Gitaly->>Object Storage: write compacted backup to a new object "https://<object_storage_url>?id=repo_2_full_backup_2"
  Object Storage->>Gitaly: HTTP 200 OK
  Gitaly->>Rails: OK
  Rails->>Database: insert {"id": 3, "repo_id": 1, "url": "https://<object_storage_url>?id=repo_2_full_backup_2", "parent_id": none}
  Rails->>Database: delete ids 1,2
```

An occassional compaction job (let's say it's nightly) would compact incremental backups into a full backup.

### In the event of database corruption

If we use the following bucket and object id scheme, then it would still be possible to recover repositories even if there is corruption in the database.

![Bucket Naming Scheme]
(img/backup_buckets.png)

![Object Naming Scheme]
(img/backups_incr_objects.png)

Each repository has two buckets, one for full backups and one for incremental. The incremental backups are also named with a monotimically increasing
id along with the full backup id they were created from.  This way, we can develop a gitaly command that can still do compaction as well as repository
restoration.




