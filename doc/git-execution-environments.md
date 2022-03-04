# Methods for Gitaly to access Git

Because Git is at the heart of almost all interfaces provided by Gitaly, Gitaly
must have access to Git. Gitaly supports three ways of accessing Git:

- Bundled Git (recommended): Gitaly accesses bundled Git binaries. Unlike the
  other two ways of accessing Git, this is not a complete Git installation but
  only contains a subset of binaries that are required at runtime.
- External Git distribution (not recommended): Gitaly accesses Git provided by
  the operating system or manually installed by the administrator.
- Gitaly Git distribution: Gitaly accesses it's own version of Git that may
  carry custom patches which are recommended, but not required. These patches
  may fix known bugs or fix performance issues.

The bundled Git execution environment is the recommended way to set up Gitaly.

## External Git Distribution

You can run Gitaly with an external Git distribution that is not provided by
Gitaly itself. For example:

- Git distributions provided by the operating system's own package repositories.
- A custom version of Git installed by the system administrator.

To configure Gitaly to use an external Git distribution, point the Git binary
path in its configuration file to the Git executable:

```toml
[git]
bin_path = "/usr/bin/git"
```

If the binary path is not configured and Gitaly is not configured to use bundled
binaries, Gitaly tries to resolve a Git executable automatically using the
`PATH` variable. Relying on this behavior is not recommended and results in a
warning on start up.

External Git distributions must meet a minimum required version (for example,
for [GitLab 13.11.0](https://docs.gitlab.com/ee/update/#13110)) which can change
when upgrading Gitaly. Gitaly refuses to boot if the external Git distribution
does not fulfill this version requirement.

## Gitaly Git Distribution

Gitaly provides its own version of Git that may contain a custom set of patches.
These patches fix known issues we experience with Gitaly and may fix performance
issues we have observed.

While we backport patches that have been accepted in the upstream Git project,
we do not plan to apply patches that will not eventually end up in Git itself.

To use Gitaly's Git distribution, run `make git`. This Make target automatically
fetches, builds, and installs Git into a specific directory that is configurable
with the `GIT_PREFIX` environment variable. For example, to install the Git
distribution into `/usr/local`:

```shell
make git GIT_PREFIX=/usr/local
```

Then configure the Git distribution the same way as an external Git
distribution.

## Bundled Git (recommended)

Instead of using a full Git distribution, you can use a bundled Git execution
environment.

To install bundled Git binaries, run `make install-bundled-git`. This Make
target installs the bundled Git binaries in the `bin` directory of a location
set with the `PREFIX` environment variable. For example:

```shell
make install-bundled-git PREFIX=/usr/local
```

Bundled Git binaries all have a `gitaly-` prefix so they do not clash with
normal Git executables. Furthermore, bundled Git binaries may have a version
suffix which allows Gitaly to install multiple different versions of Git at the
same time. For example, `make install-bundled-git PREFIX=/usr/local` may install
`gitaly-git-v2.35.1` and `gitaly-git-v2.36.0` into `/usr/local/bin`.

Being able to install multiple Git versions in parallel solves two important
issues:

- When doing zero-downtime upgrades, a new Gitaly package can avoid replacing
  Git binaries that are used by the currently running version of Gitaly. Gitaly
  doesn't need to run in an intermediate state where the old version of Gitaly
  is unexpectedly using newer Git binaries.
- Gitaly can roll out new Git versions with the help of feature flags. This
  means we can roll back to old versions of Git if a new version is exhibiting
  faulty behavior.

Git binaries expect to be able to locate helper binaries at runtime. However,
because the bundled Git binaries all have a `gitaly-` prefix, the mechanisms Git
has to locate those helpers don't work.

To fix this, Gitaly has to bootstrap a Git execution environment on startup by:

- Creating a temporary directory.
- For each binary Git expects to exist, symlinking it into place.

To tell Git where to find those symlinks, we run all Git commands with the
`GIT_EXEC_PATH` environment variable that points into that temporary execution
environment.

Gitaly can be configured to use bundled binaries by setting the following keys:


```toml
# The binary directory where Gitaly's own binaries are installed. This directory
will also contain the bundled Git binaries.
bin_dir = /usr/local/bin

[git]
use_bundled_binaries = true
```
