## Beginner's guide to Gitaly contributions

### Setup

#### GitLab

Before you can develop on Gitaly, it's required to have a
[GitLab Development Kit][gdk] properly installed. After installing GitLab, verify
it to be working by starting the required servers and visiting GitLab on
`http://localhost:3000`.

#### Gitaly Proto

GitLab will want to read and manipulate Git data, to do this it needs to talk
to Gitaly. For GitLab and Gitaly it's important to have a set protocol. This
protocol defines what requests can be made and what data the requester has to
send with the request. For each request the response is defined too.

The protocol definitions can be found in `proto/*.proto`.

#### Gitaly

Gitaly is a component that calls procedure on the Git data when it's requested
to do so.

You can find a clone of the gitaly repository in
`/path/to/gdk/gitaly`. You can check out your working branch here, but
be aware that `gdk update` will reset it to the tag specified by
`/path/to/gdk/gitlab/GITALY_SERVER_VERSION`.

If you do a lot of Gitaly development this can get annoying. If you
want to stop `gdk update` from messing with your Gitaly checkout, put
the following in `/path/to/gdk/gdk.yml`:

```yaml
gitaly:
  auto_update: false
```

### Development

#### General advice

##### Using the Makefile

Gitaly uses make to manage its build process, where all targets are defined in
our top-level [Makefile](../Makefile). By default, simply running `make` will
build our "all" target, which installs Gitaly into the top-level directory so
that it's easily picked up by the GDK. The following is a list of the most
frequently used targets:

- build: Build Gitaly, but do not install it.

- install: Build and install Gitaly. The destination directory can be modified
  by modifying a set of variables, most importantly `PREFIX`.

- test: Execute both Go and Ruby tests.

- clean: Remove all generated build artifacts.

You can modify some parts of the build process by setting up various variables.
For example, by executing `make V=1` you can do a verbose build or by overriding
the `PROTOC_VERSION` and `PROTOC_HASH` a different protobuf compiler version
will be used for generating code.

If you wish to persist your configuration, you may create a `config.mak` file
next to the Makefile and put all variables you wish to override in there.

##### Editing code and seeing what happens

If you're used to Ruby on Rails development you may be used to a "edit
code and reload" cycle where you keep editing and reloading until you
have your change working. This is usually not the best workflow for Gitaly
development.

At some point you will know which Gitaly RPC you need to work on. This
is where you probably want to stop using `localhost:3000` and zoom in on
the RPC instead.

To experiment with changing an RPC you should use the Gitaly service
tests. The RPC you want to work on will have tests somewhere in
`internal/gitaly/service/...`. Find the tests for your RPC. Next, before you
edit any code, make sure the tests pass when you run them:
`go test ./internal/gitaly/service/foobar -count 1 -run MyRPC`. In this
command, `MyRPC` is a regex that will match functions like
`TestMyRPCSuccess` and `TestMyRPCValidationFailure`. Once you have found
your tests and your test command, you can start tweaking the
implementation or adding test cases and re-running the tests. The cycle
is "edit code, run tests".

This is many times faster than "edit gitaly, reinstall Gitaly into GDK,
restart, reload `localhost:3000`".

Regardless, if you do want to see your locally changed Gitaly in
action on `localhost:3000`, you can. Run the following commands in
your GDK directory:

```shell
make gitaly-setup
gdk restart gitaly
```

#### Process

In general there are a couple of stages to go through, in order:
1. Add a request/response combination to [Gitaly Proto][gitaly-proto], or edit
  an existing one
1. Change [Gitaly][gitaly] accordingly
1. Use the endpoint in other GitLab components (CE/EE, GitLab Workhorse, etc.)


##### Configuration changes

When modifying Gitaly's or Praefect's configuration, the changes should be propagated to other GitLab projects that
rely on them:

1. [gitlab/omnibus-gitlab](https://gitlab.com/gitlab-org/omnibus-gitlab) contains template files that are used to generate Gitaly's and Praefect's configuration.
2. [gitlab/CNG](https://gitlab.com/gitlab-org/build/CNG) contains configuration required to run Gitaly in a container.

##### Gitaly Proto

The [Protocol buffer documentation][proto-docs] combined with the
`*.proto` files in the `proto/` directory should
be enough to get you started. A service needs to be picked that can
receive the procedure call. A general rule of thumb is that the
service is named either after the Git CLI command, or after the Git
object type.

If either your request or response data can exceed 100KB you need to use the
`stream` keyword. To generate the server and client code, run `make proto`.

##### Gitaly

If proto is updated, run `make`. This should compile successfully.

##### Gitaly-ruby

Gitaly is mostly written in Go but it also uses a pool of Ruby helper
processes. This helper application is called gitaly-ruby and its code
is in the `ruby` subdirectory of Gitaly. Gitaly-ruby is a gRPC server,
just like its Go parent process. The Go parent proxies certain
requests to gitaly-ruby.

Currently (GitLab 10.8) it is our experience that gitaly-ruby is
unsuitable for RPC's that are slow, or that are called at a high rate.
It should only be used for:

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

There is no autoloader in gitaly-ruby. If you add new ruby files, you need to manually
add a `require` statement in `ruby/lib/gitlab/git.rb` or `ruby/lib/gitaly_server.rb.`

### Testing

Gitaly's tests are mostly written in Go but it is possible to write RSpec tests too.

Generally, you should always write new tests in Go even when testing Ruby code,
since we're planning to gradually rewrite everything in Go and want to avoid
having to rewrite the tests as well.

To run the full test suite, use `make test`.
You'll need some [test repositories](test_repos.md), you can set these up with `make prepare-tests`.

#### Go tests

- each RPC must have end-to-end tests at the service level
- optionally, you can add unit tests for functions that need more coverage

A typical set of Go tests for an RPC consists of two or three test
functions: a success test, a failure test (usually a table driven test
using t.Run), and sometimes a validation test. Our Go RPC tests use
in-process test servers that only implement the service the current
RPC belongs to. So if you are working on an RPC in the
'RepositoryService', your tests would go in
`internal/gitaly/service/repository/your_rpc_test.go`.

##### Running one specific Go test

When you are trying to fix a specific test failure it is inefficient
to run `make test` all the time. To run just one test you need to know
the package it lives in (e.g. `internal/gitaly/service/repository`) and the
test name (e.g. `TestRepositoryExists`).

To run the test you need a terminal window with working directory
`/path/to/gdk/gitaly`. To run just the one test you're interested in:

```
go test ./internal/gitaly/service/repository -count 1 -run TestRepositoryExists
```

When writing tests, prefer using [testify]'s [require], and [assert] as
methods to set expectations over functions like `t.Fatal()` and others directly
called on `testing.T`.

[testify]: https://github.com/stretchr/testify
[require]: https://github.com/stretchr/testify/tree/master/require
[assert]: https://github.com/stretchr/testify/tree/master/assert

##### Troubleshooting

There is a [known issue][] running Go tests while using the [asdf version
manager][asdf].

In order to avoid polluting local configurations during testing, a test may
redefine the location of `$HOME`, which interferes with asdf's `$ASDF_DATA_DIR`
definition of `$HOME/.asdf`. You may see errors like this:

```
unknown command: bundle. Perhaps you have to reshim?
```

As a workaround, explicitly define the variable prior to executing tests:

```sh
$ ASDF_DATA_DIR=~/.asdf go test ...
```

[known issue]: https://gitlab.com/gitlab-org/gitaly/-/issues/2646
[asdf]: https://asdf-vm.com/

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
