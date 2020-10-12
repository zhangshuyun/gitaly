# RFC: Proposal to Support Real Time Git Applications

## Abstract

There are many "real time" use cases for Git repositories that are not
performant on GitLab for a number of reasons. Supporting more performant
cloning and employing asynchronous push notifications of changes can help
mitigate the short comings of Git to open up the door to new applications.
<REPLACE ABSTRACT>

## Real-time applications of Git

Real-time applications of Git are applications where rapid reactions to changing
data are required. These applications require the ability to know when data
changes, and the ability to update data quickly.

An existing real-time application is GitLab CI. GitLab CI rapidly
responds to updates to branches in a repository. It accomplishes this through
Git hooks fired in Gitaly after an update which notify the GitLab Rails web
application to queue an asynchronous CI job.

A potential application could be a chat app where many users are updating a
common space (e.g. chat room) and other participants want to be notified as soon
as possible of new data.

## Git and GitLab shortcomings for real time applications

### Clone performance

When cloning a repository, the server determines the ideal pack file to send
the client based on what the client wants. In the case of real-time
applications, the client may only be interested in a changes from a small number
of references. The server must isolate the changes needed for client among many
other changes that may be irrelevant. This is time consuming work that may span
many pack files to find a small amount of data.

### Determining reference updates

References (refs) tie together a repository by defining which objects are
relevant. A client of a real-time application will typically be interested in
some of all of the refs in a repository. Currently, customers interested in
repository changes resort to a number of strategies:

- Asynchronous push notifications
  - Webhooks - slow, and require a new session to be established for each
    notification.
  - Install custom hooks - a configuration management nightmare for admins and
    GitLab support.
- Synchronous polling
  - git ls-remote - fetches desired refs
  - git fetch - fetches refs and objects

The current solutions are web-hooks and ref polling. Web-hooks are very slow
due to being queued by Rails and then later executed by Sidekiq at an oppurtune
time. Polling puts stress on Gitaly by unnecessarily consuming resources when
repositories are unchanged.

### Streaming Files in New Commits

Another shortcoming in GitLab is the inability to write changes to a repository
in real time. Typically, changes are written to a repository as complete files.
The entire file contents is transferred to Gitaly and placed into quarantine
before authorizing the change. This means that it is not possible to stream a
large file into a repository without first storing the entire file in memory or
on disk. Git has the ability to stream a blob to the object store (see `git
hash-object -w`) however there is no guarantee it will not be reclaimed by
garbage collection depending on the repository configuration.
