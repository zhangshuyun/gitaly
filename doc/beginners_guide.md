## Beginner's guide to Gitaly contributions

### Setup

#### GitLab

Before you can develop on Gitaly, it's required to have the
[GitLab Development Kit][gdk] properly installed. After installing GitLab, verify
it to be working by starting the required servers and visiting GitLab at
`http://localhost:3000`.

#### Gitaly Proto

Data is shared between GitLab Rails and Gitaly using the [Google Protocol Buffers](https://developers.google.com/protocol-buffers) to provide a shared format for serializing the exchanged data. They are also referred to as _protobuf_.

Protocol buffers define which requests can be made and what data the requester must provide with the request. The response to each request is likewise defined using the Protocol buffers format.

The protocol definitions can be found in `proto/*.proto`.

#### Gitaly

Gitaly provides high-level RPC access to Git repositories. It controls access to the `git` binary and is used by GitLab to read and write Git data. Gitaly is present in every GitLab installation and coordinates Git repository storage and retrieval.

Within the GDK, you can find a clone of the Gitaly repository in
`/path/to/gdk/gitaly`. You can check out your working branch here, but
please be aware that `gdk update` will reset it to the tag specified by
`/path/to/gdk/gitlab/GITALY_SERVER_VERSION`.

This can be ineffective if you do a lot of Gitaly development, so if you
want to stop `gdk update` from overwriting your Gitaly checkout, add
the following to `/path/to/gdk/gdk.yml`:

```yaml
gitaly:
  auto_update: false
```

### Development

#### General advice

##### Using the Makefile

Gitaly uses [Make](https://en.wikipedia.org/wiki/Make_(software)) to manage its build process, and all targets are defined in
our top-level [Makefile](../Makefile). By default, simply running `make` will
build our `all` target, which installs Gitaly into the `./_build/bin` directory so
that it's easily picked up by the GDK. The following is a list of the most
frequently used targets:

- `build`: Build Gitaly, but do not install it.

- `install`: Build and install Gitaly. The destination directory can be modified
  by modifying a set of variables, most importantly `PREFIX`.

- `test`: Execute both Go and Ruby tests.

- `clean`: Remove all generated build artifacts.

You can modify some parts of the build process by setting up various variables.
For example, by executing `make V=1` you can do a verbose build or by overriding
the `PROTOC_VERSION` and `PROTOC_HASH` a different protobuf compiler version
will be used for generating code.

If you wish to persist your configuration, you may create a `config.mak` file
next to the Makefile and put all variables you wish to override in there.

##### Experimenting with editing code

If you're used to Ruby on Rails development you may be used to a "edit
code and reload" cycle where you keep editing and reloading until you
have your change working. This is usually not the best workflow for Gitaly
development.

At some point you will know which Gitaly RPC you need to work on. This
is where you probably want to stop using `localhost:3000` and zoom in on
the RPC instead.

###### A suggested workflow

To experiment with changing an RPC you should use the Gitaly service
tests. The RPC you want to work on will have tests somewhere in
`internal/gitaly/service/...`.

Before you edit any code, make sure the tests pass when you run them:

```
TEST_PACKAGES=./internal/gitaly/service/foobar TEST_OPTIONS="-count=1 -run=MyRPC" make test
```

In this command, `MyRPC` is a regex that will match functions like
`TestMyRPCSuccess` and `TestMyRPCValidationFailure`.

Once you have found your tests and your test command, you can start tweaking the implementation or adding test cases and re-running the tests.

This approach is many times faster than "edit gitaly, reinstall Gitaly into GDK,
restart, reload `localhost:3000`".

To see the changes, run the following commands in
your GDK directory:

```shell
make gitaly-setup
gdk restart gitaly
```

#### Development Process

The general approach is:
1. Add a request/response combination to [Gitaly Proto][gitaly-proto], or edit
  an existing one
1. Change [Gitaly][gitaly] accordingly
1. Use the endpoint in other GitLab components (CE/EE, GitLab Workhorse, etc.)


##### Configuration changes

When modifying the Gitaly or Praefect configuration, the changes should be propagated to other GitLab projects that rely on them:

1. [gitlab/omnibus-gitlab](https://gitlab.com/gitlab-org/omnibus-gitlab) contains template files that are used to generate Gitaly's and Praefect's configuration.
2. [gitlab/CNG](https://gitlab.com/gitlab-org/build/CNG) contains configuration required to run Gitaly in a container.

##### Gitaly Proto

The [Protocol buffer documentation][proto-docs] combined with the `*.proto` files in the `proto/` directory should be enough to get you started. A service needs to be picked that can receive the procedure call. A general rule of thumb is that the service is named either after the Git CLI command, or after the Git object type.

If either your request or response data can exceed 100KB you need to use the `stream` keyword. To generate the server and client code, run `make proto`.

##### Gitaly

If proto is updated, run `make`. This should compile successfully.

##### Gitaly-ruby

Gitaly is mostly written in Go but it also uses a pool of Ruby helper
processes. This helper application is called gitaly-ruby and its code
is in the `ruby` subdirectory of Gitaly. Gitaly-ruby is a gRPC server,
just like its Go parent process. The Go parent proxies certain
requests to gitaly-ruby.

It is our experience that gitaly-ruby is unsuitable for RPC's that are slow, or that are called with a high frequency. It should only be used for:

- legacy GitLab application code that is too complex or subtle to rewrite in Go
- prototyping (if you the contributor are uncomfortable writing Go)

Note that for any changes to `gitaly-ruby` to be used by GDK, you need to
run `make gitaly-setup` in your GDK root and restart your processes.

###### Gitaly-ruby boilerplate

To create the Ruby endpoint, some Go is required as the go code receives the
requests and proxies it to the Go server. In general this is boilerplate code
where only method and variable names are different.

Examples:
- Simple: [Simple request in, simple response out](https://gitlab.com/gitlab-org/gitaly/blob/6841327adea214666417ee339ca37b58b20c649c/internal/service/wiki/delete_page.go)
- Client Streamed: [Stream in, simple response out](https://gitlab.com/gitlab-org/gitaly/blob/6841327adea214666417ee339ca37b58b20c649c/internal/service/wiki/write_page.go)
- Server Streamed: [Simple request in, streamed response out](https://gitlab.com/gitlab-org/gitaly/blob/6841327adea214666417ee339ca37b58b20c649c/internal/service/wiki/find_page.go)
- Bidirectional: No example at this time

###### Ruby

The Ruby code needs to be added to `ruby/lib/gitaly_server/<service-name>_service.rb`.
The method name should match the name defined by the `gitaly` gem. To be sure
run `bundle open gitaly`. The return value of the method should be an
instance of the response object.

There is no autoloader in gitaly-ruby. If you add new ruby files, you need to manually add a `require` statement in `ruby/lib/gitlab/git.rb` or `ruby/lib/gitaly_server.rb.`

### Testing

Gitaly's tests are mostly written in Go but it is possible to write RSpec tests too.

Generally, you should always write new tests in Go even when testing Ruby code,
since we're planning to gradually rewrite everything in Go and want to avoid
having to rewrite the tests as well.

Because Praefect lives in the same repository we need to provide database connection
information in order to run tests for it successfully. To get more info check out
[glsql](../internal/praefect/datastore/glsql/doc.go) package documentation.

The easiest way to set up a Postgres database instance is to run it as a Docker container:

```bash
docker rm -f $(docker ps -q --all -f name=praefect-pg) > /dev/null 2>1; \
docker run --name praefect-pg -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust -d postgres:12.6
```

To run the full test suite, use `make test`.
You'll need some [test repositories](test_repos.md), you can set these up with `make prepare-tests`.

#### Go tests

- each RPC must have end-to-end tests at the service level
- optionally, you can add unit tests for functions that need more coverage

##### Integration Tests

A typical set of Go tests for an RPC consists of two or three test
functions:

- a success test
- a failure test (usually a table driven test using t.Run)
- sometimes a validation test.

Our Go RPC tests use in-process test servers that only implement the service the current RPC belongs to.

For example, if you are working on an RPC in the 'RepositoryService', your tests would go in `internal/gitaly/service/repository/your_rpc_test.go`.

##### Running a specific test

When you are trying to fix a specific test failure it is inefficient
to run `make test` all the time. To run just one test you need to know
the package it lives in (e.g. `internal/gitaly/service/repository`) and the
test name (e.g. `TestRepositoryExists`).

To run the test you need a terminal window with working directory
`/path/to/gdk/gitaly`. To run just the one test you're interested in:

```
TEST_PACKAGES=./internal/gitaly/service/repository TEST_OPTIONS="-count=1 -run=TestRepositoryExists" make test-go
```

When writing tests, prefer using [testify]'s [require], and [assert] as
methods to set expectations over functions like `t.Fatal()` and others directly
called on `testing.T`.

[testify]: https://github.com/stretchr/testify
[require]: https://github.com/stretchr/testify/tree/master/require
[assert]: https://github.com/stretchr/testify/tree/master/assert

##### Useful snippets for creating a test

###### testhelper package

The `testhelper` package provides functions to create configurations to run gitaly and helpers to run a Gitaly gRPC server:

- [Create test configuration](https://gitlab.com/gitlab-org/gitaly/-/blob/aa098de7b267e3d6cb8a05e7862a1ad34f8f2ab5/internal/gitaly/service/ref/testhelper_test.go#L43)
- [Run Gitaly](https://gitlab.com/gitlab-org/gitaly/-/blob/aa098de7b267e3d6cb8a05e7862a1ad34f8f2ab5/internal/gitaly/service/ref/testhelper_test.go#L57)
- [Clone test repository](https://gitlab.com/gitlab-org/gitaly/-/blob/aa098de7b267e3d6cb8a05e7862a1ad34f8f2ab5/internal/gitaly/service/ref/find_all_tags_test.go#L30)

#### RSpec tests

It is possible to write end-to-end RSpec tests that run against a full
Gitaly server. This is more or less equivalent to the service-level
tests we write in Go. You can also write unit tests for Ruby code in
RSpec.

Because the RSpec tests use a full Gitaly server you must re-compile
Gitaly every time you change the Go code. Run `make` to recompile.

Then, you can run RSpec tests in the `ruby` subdirectory.

```
cd ruby
bundle exec rspec
```

### Rails tests

To use your custom Gitaly when running Rails tests in GDK, go to the
`gitlab` directory in your GDK and follow the instructions at
[Running tests with a locally modified version of Gitaly][custom-gitaly].

[custom-gitaly]: https://docs.gitlab.com/ee/development/gitaly.html#running-tests-with-a-locally-modified-version-of-gitaly
[gdk]: https://gitlab.com/gitlab-org/gitlab-development-kit/#getting-started
[git-remote]: https://git-scm.com/book/en/v2/Git-Basics-Working-with-Remotes
[gitaly]: https://gitlab.com/gitlab-org/gitaly
[gitaly-proto]: https://gitlab.com/gitlab-org/gitaly/tree/master/proto
[gitaly-issue]: https://gitlab.com/gitlab-org/gitaly/issues
[gitlab]: https://gitlab.com
[go-workspace]: https://golang.org/doc/code.html#Workspaces
[proto-docs]: https://developers.google.com/protocol-buffers/docs/overview
