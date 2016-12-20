# Gitaly

- [About the name Gitaly](#name)

## Summary

Gitaly is an abstraction and caching layer on top of Git. It serves as a GitRPC
daemon that reduces IOPS by aggresively caching expensive git operations and providing
an API so GitLab can perform queries for metadata and serve them directly from memory
instead of reaching the filesystem.

[Reasoning and design](design/README.md)

## Name

Gitaly is a tribute to git and the town of [Aly][aly-wiki]. Where the town of
Aly has zero inhabitants most of the year we would like to reduce the number of
disk operations to zero for most actions. It doesn't hurt that it sounds like
Italy, the capital of which is [the destination of all roads][rome].
All git paths lead to Gitaly.

[aly-wiki]: https://en.wikipedia.org/wiki/Aly
[rome]: https://en.wikipedia.org/wiki/All_roads_lead_to_Rome
