# Proposal: authentication via identities

## High level summary

In order to authenticate access to both Gitaly and Praefect, we currently make
use of authentication tokens. Those tokens are configured by the administrator
inside the respective configuration files, where each node needs to know about
the tokens by their respective peers it intends to reach out to.

In some cases, we have reached the limit of what we can achieve with this simple
authentication scheme. This RFC is thus going to propose an alternative
authentication scheme based on identities and certificates instead of on tokens
to solve these limitations.

Initially, this mechanism may only be used for Gitaly-specific components like
Gitaly, Praefect, the Ruby sidecard and hooks. Eventually, we may expand the
scope to also correctly authenticate other services which directly interface
with any of Gitaly's components.

## Current limitations

This section details the most important limitations which we currently face with
authentication tokens.

### Transfer of tokens via the network

In order to authenticate against Gitaly or Praefect, we need to know about the
token used to protect access to the interface. In most contexts we have those
tokens available via the components' configuration file, where the administrator
configures all tokens required to reach out to respective services.

With the introduction of Praefect and reference transactions this is no longer
true. Praefect is intended as a (mostly) transparent proxy to Gitaly and as a
result Gitaly doesn't typically know about the intermediate Praefect proxy. But
in order to achieve strong consistency, there had to be a rendezvous point
available where multiple Gitaly nodes can coordinate in order to assure that all
nodes agree on the same data. This rendezvous point nowadays is the transaction
hosted by Praefect servers.

Strong consistency thus requires Gitaly nodes to reach out to Praefect in order
to vote on data which is to be committed to disk. As Gitaly wouldn't know about
the Praefect server in the first place, Praefect has to encode connection
information into the proxied stream, which Gitaly in turn extracts and uses to
reach out to Praefect again.

As all calls to Praefect need to be authenticated, the encoded information also
contains the Praefect authentication token. We're thus transmitting credentials
to Gitaly nodes via the network. As encryption is currently optional, those
credentials may be transmitted in the clear and/or logged. It goes without
saying that this is bad practice.

### Scalability

With the introduction of high availability to Gitaly, setups become increasingly
complex. Instead of a single Gitaly node, future deployments which have strong
scaling requirements may easily deploy at least six different nodes (three
Praefect nodes, three Gitaly nodes), if not more. In such a setup, having static
secrets makes it harder than necessary to guarantee safety across all nodes.

In the easiest setup each node would have the same secret. While easy to set up,
this brings with it multiple downsides with regards to security. First, if any
node becomes compromised, all the other nodes are compromised automatically,
too. Second, authentication token rotation becomes infeasible with growing
deployment size as the token would need to be swapped for each component at the
same time.

More complex setups may have a separate authentication token for each of the
nodes. While this improves security and allows a single node to be compromised
without impacting the others directly, it is hard to maintain in the long run.
For each newly deployed node, all the other nodes which interface with it need
to be reconfigured in order to get the new token.

### Revocation

This is mostly the same point as the previous section about scalability.
Revocation of authentication tokens is currently hard to achieve as one would
either have to change configuration of all hosts or regenerate a new shared
secret altogether.

### Authorization

While achievable via authentication tokens, we currently don't enforce any
authorization of authenticated peers. As soon as a client presents the
authentication token it is allowed to invoke all RPCs of the current node.

An authorization mechanism would instead group RPCs by scope. Examples would be:

    - Gitaly's internal API, which may only be called by other Gitaly services.

    - Praefect reference transaction API, which may only be called by Gitaly.

    - Praefect and Gitaly's "public" API, which may only be called by GitLab
      Rails.

A node thus needs to be able to authenticate its remote peer and deduct further
information about it in order to be able to authorize it for a call to a given
RPC function.

## Requirements

Going by above limitations, the following requirements can be derived:

    - Each client must have its own key.

    - Client keys need to carry sufficient information in order to identify the
      role of this client.

    - It must be possible to add aditional nodes to the network without having
      to reconfigure all nodes.

    - It should be possible to remove nodes and revoke its permissions.

    - Credentials shall not be transmitted via the network.

## Proposal

### General

We are (optionally) using TLS to encrypt gRPC streams between nodes, but right
now we only make use of server certificates. As a result, we only have one-sided
authentication where clients aren't authenticated at all except for the token
they present after they are connected. We can expand this scheme to also make
use of client-side certificates, which ticks off all requirements stated above:

    - Each client can have its own private certificate.

    - Certificates may carry additional information like e.g. a subject. This
      information can be used to establish the role of a connecting peer.

    - Depending on the setup, it is possible to provision new nodes and generate
      additional certificates via a certificate authority. Each node would then
      only have the root certificate available, which it uses to authenticate
      newly added peers without a configuration change.

    - It is possible to generate certificate revocation lists in order to revoke
      any permissions which was previously granted to a potentially compromised
      node.

    - No authentication credentials are transmitted via the network.

Using client certificates allows a lot of flexibility. For small deployments,
administrators may make use of self-signed certificates which is to deployed to
each node. For medium- and large-scale deployments, a certificate authority can
be used to create and revoke certificates as needed without the need to deploy
client certificates across the network.

### Plan

The following is a potential plan for how to transition towards the use of
client certificates:

    1. Establish a scheme how a client's role shall be established. It is
       probably feasible to use the certificate subject for this, where it could
       be either e.g. "gitaly", "praefect", "gitlab" or the likes.

    2. Allow configuration and use of client certificates. At this point,
       neither client- nor server-side certificates are mandatory.

    3. Add a configuration key to optionally enforce use of client and server
       certificates.

    4. Group RPC calls by who is expected to call it and implement authorization
       based on provided client certificate. This needs to be optional at this
       point in time.

    5. Enforce use of TLS for all connections.

    6. Remove the use of authentication tokens. As all calls are now
       authenticated and authorized via client certificates, they don't serve
       any purpose anymore.

Initially, such a transition may only happen internally between Gitaly-internal
boundaries (e.g. between Praefect and Gitaly) as a proof of concept. At a later
point this may be expanded to external projects, too, e.g. to the Rails
application.

## Alternatives

### Mature our usage of tokens

Many of the current weaknesses could be solved by maturing our usage of tokens.
This would probably require the creation and distribution of additional
credentials into different configuration files and will always require to make
them available on both sides. This is hard to maintain and scale securely in a
high availability node with a swarm of Praefect and Gitaly nodes and thus deemed
as not being a proper alternative.

### Asymmetrical keys as node identities

Instead of using TLS, it is also possible to establish node identities via an
asymmetrical key pair. In this schema, authentication would be managed via
cryptographic signatures. It is similar to the use of TLS certificates, but
typically doesn't provide a way to have a central authority manage certificates
in a transparent way and probably suffers from the same scalability problems as
our current use of shared secrets. Furthermore, gRPC currently doesn't provide
any built-in way to manage such identities. An implementation would thus be more
involved compared to using client certificates.

## Conclusion

Given that we are already (optionally) using server-side certificates, adding
client-side certificates to the mix feels like the logical next step to improve
security in deployments of Gitaly and Praefect. Mutual authentication is a very
desirable property in such deployments as all participants should typically be
known ahead of time already, and additional clients can be deployed if a
certificate authority is used for the provisioning of certificates. Also,
authorization can be implemented on top via additional metadata part of the
certificate, which would allow us to get rid of shared secrets eventually.
