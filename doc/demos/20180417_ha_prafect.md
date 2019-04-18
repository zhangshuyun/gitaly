# Praefect HA Whitelist Replication Demo

This demonstration showcases the Praefect hooks for performing replication of
whitelisted repositories. While Praefect doesn't yet perform the actual
replication, we can still see the log messages that are placeholders for where
the ultimate replication implementation will execute.

## Prerequisites

- GDK installed
- Go v1.11+ installed
- Gitaly repo cloned to `$GOPATH/src/gitlab.com/gitlab-org/gitaly`

## Pre-Demo Procedure

The following is done prior to the demo to save time watching slow

1. `cd` to the GDK directory
1. `gdk init gdk-ce`
1. `cd gdk-ce`
1. `gdk install`

## Demo Procedure

The following commands are run while in the `gdk-ce` directory unless otherwise
specified:

1. Install praefect:
    - `go install $GOPATH/src/gitlab.com/gitlab-org/gitaly/cmd/praefect`
1. Open `gitlab/config/gitlab.yml` in an editor and search for first occurrence of
`gitaly_address`. Note the values for path and Unix address for Gitaly:
    ```yaml
    ## Repositories settings
    repositories:
        # Paths where repositories can be stored. Give the canonicalized absolute pathname.
        # IMPORTANT: None of the path components may be symlink, because
        # gitlab-shell invokes Dir.pwd inside the repository path and that results
        # real path not the symlink.
        storages: # You must have at least a `default` storage path.
        default:
            path: /Users/paulokstad/go/src/gitlab.com/gitlab-org/gitlab-development-kit/gdk-ce/repositories
            gitaly_address: unix:/Users/paulokstad/go/src/gitlab.com/gitlab-org/gitlab-development-kit/gdk-ce/gitaly.socket
    ```
    - For example: the following values from the above snippet should be noted:
        - name: default
        - path: /Users/paulokstad/go/src/gitlab.com/gitlab-org/gitlab-development-kit/gdk-ce/repositories
        - gitaly_address: unix:/Users/paulokstad/go/src/gitlab.com/gitlab-org/gitlab-development-kit/gdk-ce/gitaly.socket
1. Change the value in the above file for `gitaly_address` to the following: `tcp://localhost:6060`
1. Create a praefect config file in the `gdk-ce` folder titled "praefect.toml":
    ```toml
    listen_addr = "localhost:6060"
    #socket_path = ""
    whitelist = [""]
    prometheus_listen_addr = ""

    [primary_server]
    name = "default"
    listen_addr = "unix:/Users/paulokstad/go/src/gitlab.com/gitlab-org/gitlab-development-kit/gdk-ce/gitaly.socket"

    [[secondary_server]]
    name = "backup-1"
    listen_addr = "tcp://gitaly-backup1.example.com"
    ```
    1. For primary server, enter the name and address noted for the Gitaly server in the previous step.
    1. For the secondary server, leave a fake address that doesn't resolve to anything.
1. In a separate terminal in the same `gdk-ce` directory, start praefect: `praefect -config praefect.toml`
1. Open `gitlab.yml` in an editor and search for first occurrence of
`gitaly_address`. Replace the value with the socket path that praefect is
listening on.
```yaml
  ## Repositories settings
  repositories:
    # Paths where repositories can be stored. Give the canonicalized absolute pathname.
    # IMPORTANT: None of the path components may be symlink, because
    # gitlab-shell invokes Dir.pwd inside the repository path and that results
    # real path not the symlink.
    storages: # You must have at least a `default` storage path.
      default:
        path: /Users/paulokstad/go/src/gitlab.com/gitlab-org/gitlab-development-kit/gdk-ce/repositories
        gitaly_address: unix:/Users/paulokstad/go/src/gitlab.com/gitlab-org/gitlab-development-kit/gdk-ce/gitaly.socket
```
1. Open Gitaly's config file in `gitaly/config.toml` and note the storage path for the Gitaly name noted in gitlab.yml (i.e. "default"):
```toml
[[storage]]
name = "default"
path = "/Users/paulokstad/go/src/gitlab.com/gitlab-org/gitlab-development-kit/gdk-ce/repositories"
```
1. Run `gdk rub db`
1. Go to `gitlab` and run `bin/rails console`
1. Run `Project.last` and copy the project URL path (i.e. "terica_hirthe/gitlab-test")
1. Run `Project.last.disk_path` to obtain the last project's path
    - "@hashed/4e/c9/4ec9599fc203d176a301536c2e091a19bc852759b255bd6818810a42c5fed14a"
    - Copy the hashed path (note: actual path will have .git appended to folder)
1. Go back to praefect.toml and update whitelist with hashed path + .git
    ```toml
    whitelist = ["@hashed/4e/c9/4ec9599fc203d176a301536c2e091a19bc852759b255bd6818810a42c5fed14a.git"]
    ```
1. Restart praefect
    1. We expect to see praefect start a replication job for the whitelisted project hash
