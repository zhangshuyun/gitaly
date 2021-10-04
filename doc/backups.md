# Repository Backups

The `gitaly-backup` command creates repository backups.

## Legacy layout

This layout is designed to be identical to historic `backup.rake` repository
backups. Repository data is stored in bundle files in a pre-determined
directory structure based on each repository's relative path. This directory
structure is then archived into a tar file by `backup.rake`. Each time a backup
is created, this entire directory structure is recreated.

For example, a repository with the relative path of
`@hashed/4e/c9/4ec9599fc203d176a301536c2e091a19bc852759b255bd6818810a42c5fed14a.git`
creates the following structure:

```text
$BACKUP_DESTINATION_PATH/
  @hashed/
    4e/
      c9/
        4ec9599fc203d176a301536c2e091a19bc852759b255bd6818810a42c5fed14a.bundle
```


### Generating full backups

A bundle with all references is created via the RPC `CreateBundle`. It
effectively executes the following:

```shell
git bundle create repo.bundle --all
```

### Generating incremental backups

This layout does not support incremental backups.

## Pointer layout

This layout is designed to support incremental backups. Each repository backup
cannot overwrite a previous backup because this would leave dangling incremental
backups. To prevent dangling incremental backups, every new full backup is put into a new directory.
The two files called `LATEST` point to:

- The latest full backup.
- The latest increment of that full backup.

These pointer files enable looking up
backups from object storage without needing directory traversal (directory
traversal typically requires additional permissions). In addition to the bundle
files, each backup writes a full list of refs and their target object IDs.

When the pointer files are not found, the pointer layout will fall back to
using the legacy layout.

For example, a repository with the relative path of
`@hashed/4e/c9/4ec9599fc203d176a301536c2e091a19bc852759b255bd6818810a42c5fed14a.git`
and a backup ID of `20210930065413` will create the following structure:

```text
$BACKUP_DESTINATION_PATH/
  @hashed/
    4e/
      c9/
        4ec9599fc203d176a301536c2e091a19bc852759b255bd6818810a42c5fed14a/
          LATEST
          20210930065413/
            001.bundle
            001.refs
            LATEST
```

### Generating full backups

1. A full list of references is retrieved via the RPC `ListRefs`. This list is written to `001.refs` in the same format as [`git-show-ref`](https://git-scm.com/docs/git-show-ref#_output).

1. A bundle is generated using the retrieved reference names. Effectively, by running:

   ```shell
   awk '{print $2}' 001.refs | git bundle create repo.bundle --stdin
   ```
1. The backup and increment pointers are written.

### Generating incremental backups

1. The next increment is calculated by finding the increment `LATEST` file and
   adding 1. For example, `001` + `1` = `002`.

1. A full list of references is retrieved using the `ListRefs` RPC. This list is
   written to the calculated next increment (for example, `002.refs`) in the same
   format as [`git-show-ref`](https://git-scm.com/docs/git-show-ref#_output).

1. The full list of the previous increments references is retrieved by reading
   the file. For example, `001.refs`.

1. A bundle is generated using the negated list of reference targets of the
   previous increment and the new list of retrieved reference names
   by effectively running:

   ```shell
   { awk '{print "^" $1}' 001.refs; awk '{print $2}' 002.refs; } | git bundle create repo.bundle --stdin
   ```

   Negating the object IDs from the previous increment ensures that we stop
   traversing commits when we reach the HEAD of the branch at the time of the
   last incremental backup.
