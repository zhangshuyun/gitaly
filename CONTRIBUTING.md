## Developer Certificate of Origin and License

By contributing to GitLab B.V., you accept and agree to the following terms and
conditions for your present and future contributions submitted to GitLab B.V.
Except for the license granted herein to GitLab B.V. and recipients of software
distributed by GitLab B.V., you reserve all right, title, and interest in and to
your Contributions.

All contributions are subject to the Developer Certificate of Origin and license set out at [docs.gitlab.com/ce/legal/developer_certificate_of_origin](https://docs.gitlab.com/ce/legal/developer_certificate_of_origin).

_This notice should stay as the first item in the CONTRIBUTING.md file._

## Code of conduct

As contributors and maintainers of this project, we pledge to respect all people
who contribute through reporting issues, posting feature requests, updating
documentation, submitting pull requests or patches, and other activities.

We are committed to making participation in this project a harassment-free
experience for everyone, regardless of level of experience, gender, gender
identity and expression, sexual orientation, disability, personal appearance,
body size, race, ethnicity, age, or religion.

Examples of unacceptable behavior by participants include the use of sexual
language or imagery, derogatory comments or personal attacks, trolling, public
or private harassment, insults, or other unprofessional conduct.

Project maintainers have the right and responsibility to remove, edit, or reject
comments, commits, code, wiki edits, issues, and other contributions that are
not aligned to this Code of Conduct. Project maintainers who do not follow the
Code of Conduct may be removed from the project team.

This code of conduct applies both within project spaces and in public spaces
when an individual is representing the project or its community.

Instances of abusive, harassing, or otherwise unacceptable behavior can be
reported by emailing contact@gitlab.com.

This Code of Conduct is adapted from the [Contributor Covenant](https://contributor-covenant.org), version 1.1.0,
available at [https://contributor-covenant.org/version/1/1/0/](https://contributor-covenant.org/version/1/1/0/).

## Style Guide

The Gitaly style guide is [documented in it's own file](STYLE.md).

## Commits

In this project we value good commit hygiene. Clean commits makes it much
easier to discover when bugs have been introduced, why changes have been made,
and what their reasoning was.

When you submit a merge request, expect the changes to be reviewed
commit-by-commit. To make it easier for the reviewer, please submit your MR
with nicely formatted commit messages and changes tied together step-by-step.

### Write small, atomic commits

Commits should be as small as possible but not smaller than required to make a
logically complete change. If you struggle to find a proper summary for your
commit message, it's a good indicator that the changes you make in this commit may
not be focused enough.

`git add -p` is useful to add only relevant changes. Often you only notice that
you require additional changes to achieve your goal when halfway through the
implementation.  Use `git stash` to help you stay focused on this additional
change until you have implemented it in a separate commit.

### Split up refactors and behavioral changes

Introducing changes in behavior very often requires preliminary refactors. You
should never squash refactoring and behavioral changes into a single commit,
because that makes it very hard to spot the actual change later.

### Tell a story

When splitting up commits into small and logical changes, there will be many
interdependencies between all commits of your feature branch. If you make
changes to simply prepare another change, you should briefly mention the overall
goal that this commit is heading towards.

### Describe why you make changes, not what you change

When writing commit messages, you should typically explain why a given change is
being made. For example, if you have pondered several potential solutions, you
can explain why you settled on the specific implementation you chose. What has
changed is typically visible from the diff itself.

A good commit message answers the following questions:

- What is the current situation?
- Why does that situation need to change?
- How does your change fix that situation?
- Are there relevant resources which help further the understanding? If so,
  provide references.

You may want to set up a [message template](https://thoughtbot.com/blog/better-commit-messages-with-a-gitmessage-template)
to pre-populate your editor when executing `git commit`.

### Use scoped commit subjects

Many projects typically prefix their commit subjects with a scope. For example,
if you're implementing a new feature "X" for subsystem "Y", your commit message
would be "Y: Implement new feature X". This makes it easier to quickly sift
through relevant commits by simply inspecting this prefix.

### Keep the commit subject short

Because commit subjects are displayed in various command line tools by default,
keep the commit subject short. A good rule of thumb is that it shouldn't exceed
72 characters.

### Mention the original commit that introduced bugs

When implementing bugfixes, it's often useful information to see why a bug was
introduced and when it was introduced. Therefore, mentioning the original commit
that introduced a given bug is recommended. You can use `git blame` or `git
bisect` to help you identify that commit.

The format used to mention commits is typically the abbreviated object ID
followed by the commit subject and the commit date. You may create an alias for
this to have it easily available. For example:

```shell
$ git config alias.reference "show -s --pretty=reference"
$ git reference HEAD
cf7f9ffe5 (style: Document best practices for commit hygiene, 2020-11-20)
```

### Use interactive rebases to arrange your commit series

Use interactive rebases to end up with commit series that are readable and
therefore also easily reviewable one-by-one. Use interactive rebases to
rearrange commits, improve their commit messages, or squash multiple commits
into one.

### Create fixup commits

When you create multiple commits as part of feature branches, you
frequently discover bugs in one of the commits you've just written. Instead of
creating a separate commit, you can easily create a fixup commit and squash it
directly into the original source of bugs via `git commit --fixup=ORIG_COMMIT`
and `git rebase --interactive --autosquash`.

### Avoid merge commits

During development other changes might be made to the target branch. These
changes might cause a conflict with your changes. Instead of merging the target
branch into your topic branch, rebase your branch onto the target
branch. Consider setting up `git rerere` to avoid resolving the same conflict
over and over again.

### Ensure that all commits build and pass tests

To keep history bisectable using `git bisect`, you should ensure that all of
your commits build and pass tests. You can do this with interactive rebases, for
example: `git rebase -i --exec='make build format lint test'
origin/master`. This automatically builds each commit and verifies that they
pass formatting, linting, and our test suite.

### Changelog

Gitaly keeps a [changelog](CHANGELOG.md) that is generated:

- When a new release is created.
- From commit messages where a specific trailer is used.

The trailer should have the following format: `Changelog: <option>` where
`<option>` is one of:

- `added`
- `fixed`
- `changed`
- `deprecated`
- `removed`
- `security`
- `performance`
- `other`

The commit title is used to generate a changelog entry.

### Example

A great commit message could look something like:

```plaintext
package: Summarize change in 50 characters or less

The first line of the commit message is the summary. The summary should
start with a capital letter and not end with a period. Optionally
prepend the summary with the package name, feature, file, or piece of
the codebase where the change belongs to.

After an empty line the commit body provides a more detailed explanatory
text. This body is wrapped at 72 characters. The body can consist of
several paragraphs, each separated with a blank line.

The body explains the problem that this commit is solving. Focus on why
you are making this change as opposed to what (the code explains this).
Are there side effects or other counterintuitive consequences of
this change? Here's the place to explain them.

- Bullet points are okay, too

- Typically a hyphen or asterisk is used for the bullet, followed by a
  single space, with blank lines in between

- Use a hanging indent

These guidelines are pretty similar to those described in the Git Book
[1]. If you like you can use footnotes to include a lengthy hyperlink
that would otherwise clutter the text.

You can provide links to the related issue, or the issue that's fixed by
the change at the bottom using a trailer. A trailer is a token, without
spaces, directly followed with a colon and a value. Order of trailers
doesn't matter.

1. https://www.git-scm.com/book/en/v2/Distributed-Git-Contributing-to-a-Project#_commit_guidelines

Fixes: https://gitlab.com/gitlab-org/gitaly/-/issues/123
Changelog: added
Signed-off-by: Alice <alice@example.com>
```

## Gitaly Maintainers

- @8bitlife
- @avar
- @chriscool
- @jcaigitlab
- @pks-t
- @proglottis
- @samihiltunen
- @toon

## Development Process

Gitaly follows the engineering process as described in the [handbook][eng-process],
with the exception that our [issue tracker][gitaly-issues] is on the Gitaly
project and there's no distinction between developers and maintainers. Every team
member is equally responsible for a successful master pipeline and fixing security
issues.

Merge requests need to **approval by at least two
[Gitaly team members](https://gitlab.com/groups/gl-gitaly/group_members)**.

[eng-process]: https://about.gitlab.com/handbook/engineering/workflow/
[gitaly-issues]: https://gitlab.com/gitlab-org/gitaly/issues/

### Review Process

See [REVIEWING.md](REVIEWING.md).

## Gitaly Developer Quick-start Guide

See the [beginner's guide](doc/beginners_guide.md).

## Debug Logging

Debug logging can be enabled in Gitaly using `level = "debug"` under `[logging]` in config.toml.

## Git Tracing

Gitaly will reexport `GIT_TRACE*` [environment variables](https://git-scm.com/book/en/v2/Git-Internals-Environment-Variables) if they are set.

This can be an aid to debugging some sets of problems. For example, if you would like to know what git is doing internally, you can set `GIT_TRACE=true`:

Note that since git stderr stream will be logged at debug level, you need to enable debug logging in `config.toml`.

```shell
$ GIT_TRACE=true ./gitaly config.toml
...
DEBU[0015] 13:04:08.646399 git.c:322               trace: built-in: git 'gc'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.649346 run-command.c:626       trace: run_command: 'pack-refs' '--all' '--prune'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.652240 git.c:322               trace: built-in: git 'pack-refs' '--all' '--prune'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.655497 run-command.c:626       trace: run_command: 'reflog' 'expire' '--all'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.658331 git.c:322               trace: built-in: git 'reflog' 'expire' '--all'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.669787 run-command.c:626       trace: run_command: 'repack' '-d' '-l' '-A' '--unpack-unreachable=2.weeks.ago'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.672589 git.c:322               trace: built-in: git 'repack' '-d' '-l' '-A' '--unpack-unreachable=2.weeks.ago'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.673185 run-command.c:626       trace: run_command: 'pack-objects' '--keep-true-parents' '--non-empty' '--all' '--reflog' '--indexed-objects' '--write-bitmap-index' '--unpack-unreachable=2.weeks.ago' '--local' '--delta-base-offset' 'objects/pack/.tmp-60361-pack'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0015] 13:04:08.675955 git.c:322               trace: built-in: git 'pack-objects' '--keep-true-parents' '--non-empty' '--all' '--reflog' '--indexed-objects' '--write-bitmap-index' '--unpack-unreachable=2.weeks.ago' '--local' '--delta-base-offset' 'objects/pack/.tmp-60361-pack'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0037] 13:04:30.737687 run-command.c:626       trace: run_command: 'prune' '--expire' '2.weeks.ago'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0037] 13:04:30.753856 git.c:322               trace: built-in: git 'prune' '--expire' '2.weeks.ago'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0037] 13:04:31.071140 run-command.c:626       trace: run_command: 'worktree' 'prune' '--expire' '3.months.ago'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0037] 13:04:31.074736 git.c:322               trace: built-in: git 'worktree' 'prune' '--expire' '3.months.ago'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0037] 13:04:31.076135 run-command.c:626       trace: run_command: 'rerere' 'gc'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
DEBU[0037] 13:04:31.079286 git.c:322               trace: built-in: git 'rerere' 'gc'  grpc.method=GarbageCollect grpc.request.repoPath="gitlab/gitlab-design.git" grpc.request.repoStorage=default grpc.service=gitaly.RepositoryService peer.address= span.kind=server system=grpc
```

## Testing with Instrumentation

If you would like to test with instrumentation and prometheus metrics, use the `instrumented-cluster` docker compose configuration in
`_support/instrumented-cluster`. This cluster will create several services:

|*Service*|*Endpoint*|
|---------|------|
| Gitaly | [http://localhost:9999](http://localhost:9999) |
| Gitaly Metrics and pprof | [http://localhost:9236](http://localhost:9236) |
| Prometheus | [http://localhost:9090](http://localhost:9090) |
| cAdvisor | [http://localhost:8080](http://localhost:8080) |
| Grafana | [http://localhost:3000](http://localhost:3000) use default login `admin`/`admin` |

The gitaly service uses the `gitlab/gitaly:latest` image, which you need to build using `make docker` before starting the cluster.

Once you have the `gitlab/gitaly:latest` image, start the cluster from the `_support/instrumented-cluster` directory using:

```shell
docker-compose up --remove-orphans
```

Enter `^C` to kill the cluster.

Note that the Gitaly service is intentionally limited to 50% CPU and 200MB of memory. This can be adjusted in the `docker-compose.yml` file.

Once the cluster has started, it will clone the `gitlab-org/gitlab-ce` repository, for testing purposes.

This can then be used for testing, using tools like [gitaly-bench](https://gitlab.com/gitlab-org/gitaly-bench):

```shell
gitaly-bench -concurrency 100 -repo gitlab-org/gitlab-ce.git find-all-branches
```

It can also be used with profiling tools, for example [go-torch](https://github.com/uber/go-torch) for generating flame graphs, as follows:

```shell
go-torch --url http://localhost:9236 -p > flamegraph.svg
```
