# Gitaly code style

## Character set

### Avoid non-ASCII characters in developer-facing code

Code that is developer-facing only, like variables, functions or test
descriptions should use the ASCII character set only. This is to ensure that
code is accessible to different developers with varying setups.

## Errors

### Use %w when wrapping errors

Use `%w` when wrapping errors with context.

    fmt.Errorf("foo context: %w", err)

It allows to inspect the wrapped error by the caller with [`errors.As`](https://golang.org/pkg/errors/#As) and [`errors.Is`](https://golang.org/pkg/errors/#Is). More info about `errors` package capabilities could be found in the [blog post](https://blog.golang.org/go1.13-errors).

### Keep errors short

It is customary in Go to pass errors up the call stack and decorate
them. To be a good neighbor to the rest of the call stack we should keep
our errors short.

    // Good
    fmt.Errorf("peek diff line: %w", err)

    // Too long
    fmt.Errorf("ParseDiffOutput: Unexpected error while peeking: %w", err)

### Use lower case in errors

Use lower case in errors; it is OK to preserve upper case in names.

### Errors should stick to the facts

It is tempting to write errors that explain the problem that occurred.
This can be appropriate in some end-user facing situations, but it is
never appropriate for internal error messages. When your
interpretation is wrong it puts the reader on the wrong track.

Stick to the facts. Often it is enough to just describe in a few words
what we were trying to do.

### Use %q when interpolating strings

Unless it would lead to incorrect results, always use `%q` when
interpolating strings. The `%q` operator quotes strings and escapes
spaces and non-printable characters. This can save a lot of debugging
time.

## Logging

### Use context-based logging

The `ctxlogrus` package allows to extract a logger from the current
`context.Context` structure. This should be the default logging facility, as it
may carry additional context-sensitive information like the `correlation_id`
that makes it easy to correlate a log entry with other entries of the same
event.

### Errors

When logging an error, use the `WithError(err)` method.

### Use the `logrus.FieldLogger` interface

In case you want to pass around the logger, use the `logrus.FieldLogger`
interface instead of either `*logrus.Entry` or `*logrus.Logger`.

### Use snake case for fields

When writing log entries, you should use `logger.WithFields()` to add relevant
metadata relevant to the entry. The keys should use snake case:

```golang
logger.WithField("correlation_id", 12345).Info("StartTransaction")
```

### Use RPC name as log message

In case you do not want to write a specific log message, but only want to notify
about a certain function or RPC being called, you should use the function's name
as the log message:

```golang
func StartTransaction(id uint64) {
    logger.WithField("transaction_id", id).Debug("StartTransaction")
}
```

### Embed package into log entries

In order to associate log entries with a given code package, you should add a
`component` field to the log entry. If the log entry is generated in a method,
the component should be `$PACKAGE_NAME.$STRUCT_NAME`:

```golang
package transaction

type Manager struct {}

func (m Manager) StartTransaction(ctx context.Context) {
    ctxlogrus.Extract(ctx).WithFields(logrus.Fields{
        "component": "transaction.Manager",
    }).Debug("StartTransaction")
}
```

## Literals and constructors

### Use "address of struct" instead of new

The following are equivalent in Go:

```golang
// Preferred
foo := &Foo{}

// Don't use
foo := new(Foo)
```

There is no strong reason to prefer one over the other. But mixing
them is unnecessary. We prefer the first style.

### Use hexadecimal byte literals in strings

Sometimes you want to use a byte literal in a Go string. Use a
hexadecimal literal in that case. Unless you have a good reason to use
octal of course.

```golang
// Preferred
foo := "bar\x00baz"

// Don't use octal
foo := "bar\000baz"
```

Octal has the bad property that to represent high bytes, you need 3
digits, but then you may not use a `4` as the first digit. 0377 equals
255 which is a valid byte value. 0400 equals 256 which is not. With
hexadecimal you cannot make this mistake because the largest two digit
hex number is 0xff which equals 255.

## Functions

### Method Receivers

Without any good reason, methods should always use value receivers, where good
reasons include (but are not limited to) performance/memory concerns or
modification of state in the receiver. Otherwise, if any of the type's methods
requires a pointer receiver, all methods should be pointer receivers.

### Don't use "naked return"

In a function with named return variables it is valid to have a plain
("naked") `return` statement, which will return the named return
variables.

In Gitaly we don't use this feature. If the function returns one or
more values, then always pass them to `return`.

## Ordering

### Declare types before their first use

A type should be declared before its first use.

## Tests

### Naming

Prefer to name tests in the same style as [examples](https://golang.org/pkg/testing/#hdr-Examples).

To declare a test for the package, a function F, a type T and method M on type T are:

```go
func TestF() { ... }
func TestT() { ... }
func TestT_M() { ... }
```

A suffix may be appended to distinguish between test cases. The suffix must
start with a lower-case letter and use camelCasing to separate words.

```go
func TestF_suffix() { ... }
func TestT_suffix() { ... }
func TestT_M_suffix() { ... }
func TestT_M_suffixWithMultipleWords() { ... }
```

### Table-driven tests

We like table-driven tests ([Table-driven tests using subtests](https://blog.golang.org/subtests#TOC_4.), [Cheney blog post], [Golang wiki]).

-   Use [subtests](https://blog.golang.org/subtests#TOC_4.) with your table-driven tests, using `t.Run`:

```
func TestTime(t *testing.T) {
    testCases := []struct {
        gmt  string
        loc  string
        want string
    }{
        {"12:31", "Europe/Zuri", "13:31"},
        {"12:31", "America/New_York", "7:31"},
        {"08:08", "Australia/Sydney", "18:08"},
    }
    for _, tc := range testCases {
        t.Run(fmt.Sprintf("%s in %s", tc.gmt, tc.loc), func(t *testing.T) {
            loc, err := time.LoadLocation(tc.loc)
            if err != nil {
                t.Fatal("could not load location")
            }
            gmt, _ := time.Parse("15:04", tc.gmt)
            if got := gmt.In(loc).Format("15:04"); got != tc.want {
                t.Errorf("got %s; want %s", got, tc.want)
            }
        })
    }
}
```

  [Cheney blog post]: https://dave.cheney.net/2013/06/09/writing-table-driven-tests-in-go
  [Golang wiki]: https://github.com/golang/go/wiki/TableDrivenTests

### Fatal exit

Aborting test execution with any function which directly or indirectly calls
`os.Exit()` should be avoided as this will cause any deferred function calls to
not be executed. As a result, tests may leave behind testing state. Most
importantly, this includes any calls to `log.Fatal()` and related functions.

### Common setup

When all tests require a common setup, we use the `TestMain()` function for
this. `TestMain()` must call `os.Exit()` to indicate whether any tests failed.
As this will cause deferred function calls to not be processed, we use the
following pattern:

```
func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	cleanup := testhelper.Configure()
	defer cleanup()
	return m.Run()
}
```

## Black box and white box testing

The dominant style of testing in Gitaly is "white box" testing, meaning
test functions for package `foo` declare their own package also to be
`package foo`. This gives the test code access to package internals. Go
also provides a mechanism sometimes called "black box" testing where the
test functions are not part of the package under test: you write
`package foo_test` instead. Depending on your point of view, the lack of
access to package internals when using black-box is either a bug or a
feature.

As a team we are currently divided on which style to prefer so we are
going to allow both. In areas of the code where there is a clear
pattern, please stick with the pattern. For example, almost all our
service tests are white box.

## Prometheus metrics

Prometheus is a great tool to collect data about how our code behaves in
production. When adding new Prometheus metrics, please follow the [best
practices](https://prometheus.io/docs/practices/naming/) and be aware of
the
[gotchas](https://prometheus.io/docs/practices/instrumentation/#things-to-watch-out-for).

### Main function

If tests require a `TestMain()` function for common setup, this function should
be implemented in a file called `testhelper_test.go`

## Git Commands

Gitaly relies heavily on spawning git subprocesses to perform work. Any git
commands spawned from Go code should use the constructs found in
[`safecmd.go`](internal/git/safecmd.go). These constructs, all beginning with
`Safe`, help prevent certain kinds of flag injection exploits. Proper usage is
important to mitigate these injection risks:

- When toggling an option, prefer a longer flag over a short flag for
  readability.
	- Desired: `git.Flag{Name: "--long-flag"}` is easier to read and audit
	- Undesired: `git.Flag{Name: "-L"}`
- When providing a variable to configure a flag, make sure to include the
  variable after an equal sign
	- Desired: `[]git.Flag{Name: "-a="+foo}` prevents flag injection
	- Undesired: `[]git.Flag(Name: "-a"+foo)` allows flag injection
- Always define a flag's name via a constant, never use a variable:
	- Desired: `[]git.Flag{Name: "-a"}`
	- Undesired: `[]git.Flag{Name: foo}` is ambiguous and difficult to audit

## Go Imports Style

When adding new package dependencies to a source code file, keep all standard
library packages in one contiguous import block, and all third party packages
(which includes Gitaly packages) in another contiguous block. This way, the
goimports tool will deterministically sort the packages which reduces the noise
in reviews.

Example of **valid** usage:

```go
import (
	"context"
	"io"
	"os/exec"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git/alternates"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
)
```

Example of **invalid** usage:

```go
import (
	"io"
	"os/exec"

	"context"

	"gitlab.com/gitlab-org/gitaly/internal/git/alternates"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"

	"gitlab.com/gitlab-org/gitaly/internal/command"
)
```

## Goroutine Guidelines

Gitaly is a long lived process. This means that every goroutine spawned carries
liability until either the goroutine ends or the program exits. Some goroutines
are expected to run until program termination (e.g. server listeners and file
walkers). However, the vast majority of goroutines spawned are in response to
an RPC, and in most cases should end before the RPC returns. Proper cleanup of
goroutines is crucial to prevent leaks. When in doubt, you can consult the
following guide:

### Is A Goroutine Necessary?

Avoid using goroutines if the job at hand can be done just as easily and just as well without them.

### Background Task Goroutines

These are goroutines we expect to run the entire life of the process. If they
crash, we expect them to be restarted. If they restart often, we may want a way
to delay subsequent restarts to prevent resource consumption. See
[`dontpanic.GoForever`] for a useful function to handle goroutine restarts with
Sentry observability.

### RPC Goroutines

These are goroutines created to help handle an RPC. A goroutine that is started
during an RPC will also need to end when the RPC completes. This quality makes
it easy to reason about goroutine cleanup.

#### Defer-based Cleanup

One of the safest ways to clean up goroutines (as well as other resources) is
via deferred statements. For example:

```go
func (scs SuperCoolService) MyAwesomeRPC(ctx context.Context, r Request) error {
    done := make(chan struct{}) // signals the goroutine is done
    defer func() { <-done }() // wait until the goroutine is done

    go func() {
        defer close(done)    // signal when the goroutine returns
	doWork(r)
    }()

    return nil
}
```

Note the heavy usage of defer statements. Using defer statements means that
clean up will occur even if a panic bubbles up the call stack (**IMPORTANT**).
Also, the resource cleanup will
occur in a predictable manner since each defer statement is pushed onto a LIFO
stack of defers. Once the function ends, they are popped off one by one.

### Goroutine Panic Risks

Additionally, every new goroutine has the potential to crash the process. Any
unrecovered panic can cause the entire process to crash and take out any in-
flight requests (**VERY BAD**). When writing code that creates a goroutine,
consider the following question: How confident are you that the code in the
goroutine won't panic? If you can't answer confidently, you may want to use a
helper function to handle panic recovery: [`dontpanic.Go`].

### Limiting Goroutines

When spawning goroutines, you should always be aware of how many goroutines you
will be creating. While cheap, goroutines are not free. Consult the following
questions if you need help deciding if goroutines are being improperly used:

1. How many goroutines will it take the task/RPC to complete?
   - Fixed number - 👍 Good
   - Variable number - 👇 See next question...
1. Does the goroutine count scale with a configuration value (e.g. storage
   locations or concurrency limit)?
   - Yes - 👍 Good
   - No - 🚩 this is a red flag! An RPC where the goroutines do not scale
     predictably will open up the service to denial of service attacks.

## Commits

While not directly related to coding style, it's still important to have a set
of best practices around how Git commits are assembled. Proper commit hygiene
makes it much easier to discover when bugs have been introduced, why changes
have been made and what their reasoning was.

### Write small, atomic commits

Commits should be as small as possible but not smaller than required to make a
logically complete change. If you struggle to find a proper summary for your
commit message, it's a good indicator that the changes you do in this commit may
not be focussed enough.

Using `git add -p` is a great help to only add relevant changes. Often times,
you only notice you require additional changes to achieve your goal when halfway
through the implementation. Using `git stash` can help you stay focussed on this
additional change until you have implemented it in a separate commit.

### Split up refactors and behavioural changes

More often than not, introducing changes in behaviour requires preliminary
refactors. You should never squash this refactoring and behavioural change into
a single commit, as it makes it very hard to spot the actual change at later
points in time.

### Tell a story

When splitting up commits into small and logical changes, then there will be
quite some interdependencies between all commits of your topic branch. If you do
changes whose purpose it simply is to prepare another change, then you should
briefly mention the overall goal this commit is heading towards.

### Describe why you make changes, not what you change

When writing commit messages, you should typically explain why a given change is
being made. What has changed is typically visible from the diff itself. There
are obviously exceptions to that rule, e.g. if you have pondered several
potential solutions it is reasonable to explain why you have settled on the
specific implementation you chose.

A good commit message answers the following questions:

- What is the current situation?
- Why does that situation need to change?
- How does your change fix that situation?
- Are there relevant resources which help further the understanding? If so,
  provide references.

You may want to set up a [message template] to pre-populate your editor when
executing git-commit(1).

### Use scoped commit subjects

Many projects typically prefix their commit subjects with a scope. E.g. if
you're implementing a new feature "X" for subsystem "Y", your commit message
would be "Y: Implement new feature X". This makes it easier to quickly sift
through relevant commits by simply inspecting this prefix.

### Keep the commit subject short

As commit subjects are displayed in various command line tools by default, it is
recommended to keep the commit subject short. A good rule of thum is that it
shouldn't exceed 72 characters.

### Mention the original commit which has introduced bugs

When implementing bugfixes, it's often useful information to see why a bug was
introduced and when it has been introduced. Mentioning the original commit which
has introduced a given bug is thus recommended. You may use e.g. `git blame` or
`git bisect` to help you identify that commit.

The format used to mention commits is typically the abbreviated object ID
followed by the commit subject and the commit date. You may create an alias for
this to have it easily available:

```
$ git config alias.reference "show -s --pretty=reference"
$ git reference HEAD
cf7f9ffe5 (style: Document best practices for commit hygiene, 2020-11-20)
```

### Use interactive rebases to shape your commit series

Using interactive rebases is crucial to end up with commit series which are
readable and thus also easily reviewable one by one. Use it to rearrange
commits, improve their commit messages or squash multiple commits into one.

### Create fixup commits

When you create multiple commits as part of feature branches, you will
frequently discover bugs in one of the commits you've just written. Instead of
creating a separate commit, you can easily create a fixup commit and squash it
directly into the original source of bugs via `git commit --fixup=ORIG_COMMIT`
and `git rebase --interactive --autosquash`.

### Ensure that all commits build and pass tests

In order to keep history bisectable via `git bisect`, you should ensure that all
of your commits build and pass tests. You can do so with interactive rebases,
e.g. with `git rebase -i --exec='make build format lint test' origin/master`.
This will automatically build each commit and verify that it passes formatting,
linting and our test suite.

[`dontpanic.GoForever`]: https://pkg.go.dev/gitlab.com/gitlab-org/gitaly/internal/dontpanic?tab=doc#GoForever
[`dontpanic.Go`]: https://pkg.go.dev/gitlab.com/gitlab-org/gitaly/internal/dontpanic?tab=doc#Go
[message template]: https://thoughtbot.com/blog/better-commit-messages-with-a-gitmessage-template
