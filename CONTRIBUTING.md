# Contributing to Gitaly

This document describes requirements for merge requests to Gitaly.

### Changelog

Any new merge request must contain either a CHANGELOG.md entry or a
justification why no changelog entry is needed. New changelog entries
should be added to the 'UNRELEASED' section of CHANGELOG.md.

If a change is specific to an RPC, start the changelog line with the
RPC name. So for a change to RPC `FooBar` you would get:

> FooBar: Add support for `fluffy_bunnies` parameter
