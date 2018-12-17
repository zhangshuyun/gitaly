# Repositories used by the Gitaly test suite

Gitaly uses two test repositories. One should be enough but we got a
second one for free when importing code from gitlab-ce.

These repositories get cloned by `make prepare-tests`. They end up in:

-   `internal/testhelper/testdata/data/gitlab-test.git`
-   `internal/testhelper/testdata/data/gitlab-git-test.git`

To prevent fragile tests, we use fixed `packed-refs` files for these
repositories. They get installed by make (see `_support/makegen.go`)
from files in `_support`.

To update `packed-refs` run `git gc` in your test repo and copy the new
`packed-refs` to the right location in `_support`.

**TODO** define workflow "for dummies" to update packed-refs.
