# StreamRPC

StreamRPC is a remote procedure call (RPC) protocol implemented by
Gitaly. It is used for RPC's that transfer a high volume of byte stream
data, such as the server side of `git fetch`.

For background on why we created StreamRPC, see
https://gitlab.com/groups/gitlab-com/gl-infra/-/epics/463.

## Design goals

1. Give RPC handlers direct access to the underlying network socket or
TLS stream
1. Interoperate with existing Gitaly gRPC middlewares (logging,
authentication, metrics etc.)
1. Allow for efficient proxying in Praefect

## Semantics

A StreamRPC call has two phases: the handshake phase and the stream
phase. The structure of the handshake phase is described by the
StreamRPC protocol. The stream phase has no inherent structure as far
as StreamRPC is concerned; it is up to the RPC client and RPC handler
what they do with the stream.

The handshake phase consists of two steps:

1. Client sends request
1. Server sends response

To allow for a clean transition from the handshake phase to the stream
phase, the handshake phase uses frames with length prefixes. Length
prefixes make it possible to implement the handshake without buffered
IO. When the transition to the stream phase happens, we do not have to
hand over buffered data.

All length prefixes in the handshake phase are big-endian uint32
values (4 bytes). Length prefixes do not include the length of the
prefix itself, so a blob `foobar` would be encoded as
`\x00\x00\x00\x06foobar`.

### Request

The request consists of a length prefix and a JSON object.

The JSON object has three fields:

1. `Method` (`string`): this is a gRPC style method name, including the
gRPC service name.
1. `Metadata` (`map[string][]string`): this contains gRPC metadata, which
can be compared to HTTP headers. It is used for authentication,
correlation ID's, etc.
1. `Message` (`string`): this field contains a base64-encoded (RFC 4648)
Protobuf message. This is here because Praefect, and some of our
middlewares, try to inspect the first request of each RPC to see what
repository etc. it targets. By having this request as part of the
protocol, we can support Praefect and gRPC middlewares in a natural
way.

### Response

The response is a length-prefixed empty string OR a JSON object.

The server accepts the request and transitions to the stream phase if
and only if the frame is empty. That is, the accepting response is
`\x00\x00\x00\x00`.

In case of a rejection, the server returns a JSON object with an error
message in the `Error` field.

If the server rejects the request it will close the connection.

## Relation to gRPC

StreamRPC is designed to be embedded into a gRPC service (Gitaly).
StreamRPC RPC's are defined using Protobuf just like regular Gitaly
gRPC RPC's. From the point of view of gRPC middleware, StreamRPC RPC's
are unary methods which return an empty response message.

```protobuf
import "google/protobuf/empty.proto";

service ExampleService {
  rpc ExampleStream(ExampleStreamRequest) returns (google.protobuf.Empty) {
    option (op_type) = {
      op: ACCESSOR
    };
  }
}

message ExampleStreamRequest {
  Repository repository = 1 [(target_repository)=true];
}
```

The server handler may return an error after the stream phase, which
will be logged on the server, but this error cannot be transmitted to
the client. This is because the stream phase lasts until the
connection is closed. There is no way for the server to transmit the
error "after the stream phase", because the connection is then already
closed.
