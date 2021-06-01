# Protobuf specifications and client libraries for Gitaly

> This directory was previously hosted at https://gitlab.com/gitlab-org/gitaly-proto. As of Gitaly 1.58.0 and gitaly-proto 1.39.0, all further proto changes will be made here, in `gitaly/proto`.

Gitaly is part of GitLab. It is a [server
application](https://gitlab.com/gitlab-org/gitaly) that uses its own
gRPC protocol to communicate with its clients. This repository
contains the protocol definition and automatically generated wrapper
code for Go and Ruby.

The `.proto` files define the remote procedure calls for interacting
with Gitaly. We keep auto-generated client libraries for Ruby and Go
in their respective subdirectories. The list of RPCs can be
[found here](https://gitlab-org.gitlab.io/gitaly-proto/).

Run `make proto` from the root of the repository to regenerate the client
libraries after updating .proto files.

See
[developers.google.com](https://developers.google.com/protocol-buffers/docs/proto3)
for documentation of the 'proto3' Protocol buffer specification
language.

## Issues

We have disabled the issue tracker of the gitaly-proto project. Please use the
[Gitaly issue tracker](https://gitlab.com/gitlab-org/gitaly/issues).

## gRPC/Protobuf concepts

The core Protobuf concepts we use are rpc, service and message. We use
these to define the Gitaly **protocol**.

-   **rpc** a function that can be called from the client and that gets
    executed on the server. Belongs to a service. Can have one of four
    request/response signatures: message/message (example: get metadata for
    commit xxx), message/stream (example: get contents of blob xxx),
    stream/message (example: create new blob with contents xxx),
    stream/stream (example: git SSH session).
-   **service** a logical group of RPC's.
-   **message** like a JSON object except it has pre-defined types.
-   **stream** an unbounded sequence of messages. In the Ruby clients
    this looks like an Enumerator.

gRPC provides an implementation framework based on these Protobuf concepts.

-   A gRPC **server** implements one or more services behind a network
    listener. Example: the Gitaly server application.
-   The gRPC toolchain automatically generates **client libraries** that
    handle serialization and connection management. Example: the Go
    client package and Ruby gem in this repository.
-   gRPC **clients** use the client libraries to make remote procedure
    calls. These clients must decide what network address to reach their
    gRPC servers on and handle connection reuse: it is possible to
    spread different gRPC services over multiple connections to the same
    gRPC server.
-   Officially a gRPC connection is called a **channel**. In the Go gRPC
    library these channels are called **client connections** because
    'channel' is already a concept in Go itself. In Ruby a gRPC channel
    is an instance of GRPC::Core::Channel. We use the word 'connection'
    in this document. The underlying transport of gRPC, HTTP/2, allows
    multiple remote procedure calls to happen at the same time on a
    single connection to a gRPC server. In principle, a multi-threaded
    gRPC client needs only one connection to a gRPC server.

## Design decisions

1.  In Gitaly's case there is one server application
    https://gitlab.com/gitlab-org/gitaly which implements all services
    in the protocol.
1.  In default GitLab installations each Gitaly client interacts with
    exactly 1 Gitaly server, on the same host, via a Unix domain socket.
    In a larger installation each Gitaly client will interact with many
    different Gitaly servers (one per GitLab storage shard) via TCP
    connections.
1.  Gitaly uses
    [grpc.Errorf](https://godoc.org/google.golang.org/grpc#Errorf) to
    return meaningful
    [errors](https://godoc.org/google.golang.org/grpc/codes#Code) to its
    clients.
1.  Each RPC `FooBar` has its own `FooBarRequest` and `FooBarResponse`
    message types. Try to keep the structure of these messages as flat as
    possible. Only add abstractions when they have a practical benefit.
1.  We never make backwards incompatible changes to an RPC that is
    already implemented on either the client side or server side.
    Instead we just create a new RPC call and start a deprecation
    procedure (see below) for the old one.
1.  It is encouraged to put comments (starting with `//`) in .proto files.
    Please put comments on their own lines. This will cause them to be
    treated as documentation by the protoc compiler.
1.  When choosing an RPC name don't use the service name as context.
    Good: `service CommitService { rpc CommitExists }`. Bad:
    `service CommitService { rpc Exists }`.

### RPC naming conventions

Gitaly-Proto has RPCs that are resource based, for example when querying for a
commit. Another class of RPCs are operations, where the result might be empty
or one of the RPC error codes but the fact that the operation took place is
of importance.

For all RPCs, start the name with a verb, followed by an entity, and if required
followed by a further specification. For example:
- GetCommit
- RepackRepositoryIncremental
- CreateRepositoryFromBundle

For resource RPCs the verbs in use are limited to: Get, List, Create, Update,
Delete, or Is. Where both Get and List as verbs denote these operations have no side
effects. These verbs differ in terms of the expected number of results the query
yields. Get queries are limited to one result, and are expected to return one
result to the client. List queries have zero or more results, and generally will
create a gRPC stream for their results. When the `Is` verb is used, this RPC
is expected to return a boolean, or an error. For example: `IsRepositoryEmpty`.


When an operation based RPC is defined, the verb should map to the first verb in
the Git command it represents. Example; FetchRemote.

Note that the current interface defined in this repository does not yet abide
fully to these conventions. Newly defined RPCs should, though, so eventually
gitaly-proto converges to a common standard.

### Common field names and types

As a general principle, remember that Git does not enforce encodings on
most data inside repositories, so we can rarely assume data to be a
Protobuf "string" (which implies UTF-8).

1.  `bytes revision`: for fields that accept any of branch names / tag
    names / commit ID's. Uses `bytes` to be encoding agnostic.
2.  `string commit_id`: for fields that accept a commit ID.
3.  `bytes ref`: for fields that accept a refname.
4.  `bytes path`: for paths inside Git repositories, i.e., inside Git
    `tree` objects.
5.  `string relative_path`: for paths on disk on a Gitaly server,
    created by "us" (GitLab the application) instead of the user, we
    want to use UTF-8, or better, ASCII.

### Stream patterns

These are some patterns we already use, or want to use going forward.

#### Stream response of many small items

```
rpc FooBar(FooBarRequest) returns (stream FooBarResponse);

message FooBarResponse {
  message Item {
    // ...
  }
  repeated Item items = 1;
}
```

A typical example of an "Item" would be a commit. To avoid the penalty
of network IO for each Item we return, we batch them together. You can
think of this as a kind of buffered IO at the level of the Item
messages. In Go, to ease the bookkeeping you can use
[gitlab.com/gitlab-org/gitaly/internal/helper/chunker](https://godoc.org/gitlab.com/gitlab-org/gitaly/internal/helper/chunker).

#### Single large item split over multiple messages

```
rpc FooBar(FooBarRequest) returns (stream FooBarResponse);

message FooBarResponse {
  message Header {
    // ...
  }

  oneof payload {
    Header header = 1;
    bytes data = 2;
  }
}
```

A typical example of a large item would be the contents of a Git blob.
The header might contain the blob OID and the blob size. Only the first
message in the response stream has `header` set, all others have `data`
but no `header`.

In the particular case where you're sending back raw binary data from
Go, you can use
[gitlab.com/gitlab-org/gitaly/streamio](https://godoc.org/gitlab.com/gitlab-org/gitaly/streamio)
to turn your gRPC response stream into an `io.Writer`.

> Note that a number of existing RPC's do not use this pattern exactly;
> they don't use `oneof`. In practice this creates ambiguity (does the
> first message contain non-empty `data`?) and encourages complex
> optimization in the server implementation (trying to squeeze data into
> the first response message). Using `oneof` avoids this ambiguity.

#### Many large items split over multiple messages

```
rpc FooBar(FooBarRequest) returns (stream FooBarResponse);

message FooBarResponse {
  message Header {
    // ...
  }

  oneof payload {
    Header header = 1;
    bytes data = 2;
  }
}
```

This looks the same as the "single large item" case above, except
whenever a new large item begins, we send a new message with a non-empty
`header` field.

#### Footers

If the RPC requires it we can also send a footer using `oneof`. But by
default, we prefer headers.

### RPC Annotations

In preparation for Gitaly Cluster, we are now requiring all RPC's to be annotated
with an appropriate designation. All methods must contain one of the following lines:

- `option (op_type).op = ACCESSOR;`
  - Designates an RPC as being read-only (i.e. side effect free)
- `option (op_type).op = MUTATOR;`
  - Designates that an RPC modifies the repository

Failing to designate an RPC correctly will result in a CI error. For example:

`--gitaly_out: server.proto: Method ServerInfo missing op_type option`

Additionally, all mutator RPC's require additional annotations to clearly
indicate what is being modified:

- When an RPC modifies a server-wide resource, the scope should specify `SERVER`.
- When an RPC modifies a storage-wide resource, the scope should specify `STORAGE`.
  - Additionally, every request should contain field marked with `storage` annotation.
- When an RPC modifies a specific repository, the scope should specify `REPOSITORY`.
  - Additionally, every RPC with `REPOSITORY` scope, should also specify the target repository
    and may specify the additional repository.

The target repository represents the location or address of the repository
being modified by the operation. This is needed by Praefect (Gitaly Cluster) in
order to properly schedule replications to keep repository replicas up to date.

The target repository annotation marks where the target repository can be
found in the message. The annotation is added near `gitaly.Repository` field
(e.g. `Repository repository = 1 [(target_repository)=true];`). If annotated field isn't
`gitaly.Repository` type then it has to contain field annotated `[(repository)=true]` with
correct type. Having separate `repository` annotation allows to have same field in child
message annotated as both `target_repository` and `additional_repository` depending on parent
message.

The additional repository is annotated similarly to target repository but annotation
is named `additional_repository`

See our examples of [valid](go/internal/linter/testdata/valid.proto) and
[invalid](go/internal/linter/testdata/invalid.proto) proto annotations.

### Go Package

If adding new protobuf files, make sure to correctly set the `go_package` option
near the top of the file:

`option go_package = "gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb";`

This allows other protobuf files to locate and import the Go generated stubs. If
you forget to add a `go_package` option, you may receive an error similar to:

`blob.proto is missing the go_package option`

### Documentation

New or updated RPCs and message types should be accompanied by comment strings.
Good comment strings will explain why the RPC exists and how it behaves. Good
message type comments will explain what the message is communicating. Each updated
message field should have a comment.

Refer to official protobuf documentation for
[how to add comments](https://developers.google.com/protocol-buffers/docs/proto#adding_comments).

## Contributing

The CI at https://gitlab.com/gitlab-org/gitaly-proto regenerates the
client libraries to guard against the mistake of updating the .proto
files but not the client libraries. This check uses `git diff` to look
for changes. Some of the code in the Go client libraries is sensitive
to implementation details of the Go standard library (specifically,
the output of gzip). **Use the same Go version as .gitlab-ci.yml (Go
1.13)** when generating new client libraries for a merge request.

[DCO + License](CONTRIBUTING.md)

### Build process

After you change or add a .proto file you need to re-generate the Go
and Ruby libraries before committing your change.

```shell
# Re-generate Go and Ruby libraries
make proto
```

## How to deprecate an RPC call

See [DEPRECATION.md](DEPRECATION.md).

## Release

This will tag and release the gitaly-proto library, including
pushing the gem to rubygems.org

```shell
make release version=X.Y.Z
```

## How to manually push the gem

If the release script fails the gem may not be pushed. This is how you can do that after the fact:

```shell
# Use a sub-shell to limit scope of 'set -e'
(
  set -e

  # Replace X.Y.Z with the version you are pushing
  GEM_VERSION=X.Y.Z

  git checkout v$GEM_VERSION
  gem build gitaly.gemspec
  gem push gitaly-$GEM_VERSION.gem
)
```
