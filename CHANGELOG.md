# Gitaly changelog

v0.10.0

- CommitDiff: Parse a typechange diff correctly
  https://gitlab.com/gitlab-org/gitaly/merge_requests/136
- CommitDiff: Implement CommitDelta RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/139
- PostReceivePack: Set GL_REPOSITORY env variable when provided in request
  https://gitlab.com/gitlab-org/gitaly/merge_requests/137

v0.9.0

- Add support ignoring whitespace diffs in CommitDiff
  https://gitlab.com/gitlab-org/gitaly/merge_requests/126
- Add support for path filtering in CommitDiff
  https://gitlab.com/gitlab-org/gitaly/merge_requests/126

v0.8.0

- Don't error on invalid ref in CommitIsAncestor
  https://gitlab.com/gitlab-org/gitaly/merge_requests/129
- Don't error on invalid commit in FindRefName
  https://gitlab.com/gitlab-org/gitaly/merge_requests/122
- Return 'Not Found' gRPC code when repository is not found
  https://gitlab.com/gitlab-org/gitaly/merge_requests/120

v0.7.0

- Use storage configuration data from config.toml, if possible, when
  resolving repository paths.
  https://gitlab.com/gitlab-org/gitaly/merge_requests/119
- Add CHANGELOG.md
