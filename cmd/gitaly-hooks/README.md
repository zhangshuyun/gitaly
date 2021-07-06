# gitaly-hooks

`gitaly-hooks` is a binary that is the single point of entry for git hooks through gitaly.

## How is it invoked?

`gitaly-hooks` has the following subcommands:

| subcommand   | purpose                                         | arguments                            | stdin                                       |
|--------------|-------------------------------------------------|--------------------------------------|---------------------------------------------|
| `check`        | checks if the hooks can reach the gitlab server | none                                 | none                                        |
| `pre-receive`  | used as the git pre-receive hook                | none                                 | `<old-value>` SP `<new-value>` SP `<ref-name>` LF |
| `update`       | used as the git update hook                     | `<ref-name>` `<old-object>` `<new-object>` | none
| `post-receive` | used as the git post-receive hook               | none                                 | `<old-value>` SP `<new-value>` SP `<ref-name>` LF |
| `git`          | used as the git pack-objects hook               | `pack-objects` `[--stdout]` `[--shallow-file]` | `<object-list>` |

## Where is it invoked from?

There are three main code paths that call `gitaly-hooks`.

### git receive-pack (SSH & HTTP)

We have two RPCs that perform the `git receive-pack` function, [SSHReceivePack](https://gitlab.com/gitlab-org/gitaly/-/blob/master/internal/service/ssh/receive_pack.go) and [PostReceivePack](https://gitlab.com/gitlab-org/gitaly/-/blob/master/internal/service/smarthttp/receive_pack.go).

Both of these RPCs, when executing `git receive-pack`, set `core.hooksPath` to the path of the `gitaly-hooks` binary. [That happens here in `ReceivePackConfig`](https://gitlab.com/gitlab-org/gitaly/-/blob/master/internal/git/receivepack.go).

### Operations service RPCs

In the [operations service](https://gitlab.com/gitlab-org/gitaly/-/tree/master/internal/service/operations) there are RPCs that call out to `gitaly-ruby`, which then do certain operations that execute git hooks.
This is accomplished through the `with_hooks` method [here](https://gitlab.com/gitlab-org/gitaly/-/blob/master/ruby/lib/gitlab/git/operation_service.rb). Eventually the [`hook.rb`](https://gitlab.com/gitlab-org/gitaly/-/blob/master/ruby/lib/gitlab/git/hook.rb) is
called, which then calls the `gitaly-hooks` binary. This method doesn't rely on git to run the hooks. Instead, the arguments and input to the
hooks are built in ruby and then get shelled out to `gitaly-hooks`.

### git upload-pack (SSH & HTTP)

Only when Gitaly's `gitaly_pack_objects_cache_enabled` option is enabled.

SSHUploadPack and PostUploadPack, when executing `git upload-pack`, set `uploadpack.packObjectsHook` to the path of the `gitaly-hooks` binary. Afterward, when `git upload-pack` requests packfile data, it calls `gitaly-hooks` binary instead of `git pack-objects`. [That happens here in `WithPackObjectsHookEnv`](https://gitlab.com/gitlab-org/gitaly/-/blob/master/internal/git/hooks_options.go#L54)

## What does gitaly-hooks do?

`gitaly-hooks` will take the arguments and make an RPC call to `PreReceiveHook`, `UpdateHook`, `PostReceiveHook`, or `PackObjectsHook` accordingly.

**Note:**
Currently `gitaly-hooks` will only make an RPC call to `PreReceiveHook`, `UpdateHook`, or `PostReceiveHook` if a feature flag `gitaly_hook_rpc` is enabled. Otherwise, `gitaly-hooks` falls back to calling the ruby hooks directly.
