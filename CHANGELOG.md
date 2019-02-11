# Gitaly changelog

## v1.19.0

#### Fixed
- Return blank checksum for git repositories with only invalid refs
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1065

#### Other
- Use chunker in GetRawChanges
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1043

## v1.18.0

#### Other
- Make clear there is no []byte reuse bug in SearchFilesByContent
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1055
- Use chunker in FindCommits
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1059
- Statically link jaeger into Gitaly by default
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1063

## v1.17.0

#### Other
- Add glProjectPath to logs
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1049
- Switch from commitsSender to chunker
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1060

#### Security
- Disable git protocol v2 temporarily
  https://gitlab.com/gitlab-org/gitaly/merge_requests/

## v1.16.0


## v1.15.0

#### Added
- Support rbtrace and ObjectSpace via environment flags
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1046

#### Changed
- Add CountDivergingCommits RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1023

#### Fixed
- Add chunking support to SearchFilesByContent RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1015
- Avoid unsafe use of scanner.Bytes() in ref name RPC's
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1054
- Fix tests that used long unix socket paths
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1039

#### Other
- Use chunker for ListDirectories RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1042
- Stop using nil internally to signal "commit not found"
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1050
- Refactor refnames RPC's to use chunker
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1041

#### Performance
- Rewrite CommitStats in Go
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1048

## v1.14.1

#### Security
- Disable git protocol v2 temporarily
  https://gitlab.com/gitlab-org/gitaly/merge_requests/

## v1.14.0

#### Fixed
- Ensure that we kill ruby Gitlab::Git::Popen reader threads
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1040

#### Other
- Vendor gitlab-shell at 6c5b195353a632095d7f6
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1037

## v1.13.0

#### Fixed
- Fix 503 errors when Git outputs warnings to stderr
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1024
- Fix regression for https_proxy and unix socket connections
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1032
- Fix flaky rebase test
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1028
- Rewrite GetRawChanges and fix quoting bug
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1026
- Fix logging of RenameNamespace RPC parameters
  https://gitlab.com/gitlab-org/gitaly/merge_requests/847

#### Other
- Small refactors to gitaly/client
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1034
- Prepare for vendoring gitlab-shell hooks
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1020
- Replace golang.org/x/net/context with context package
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1038
- Migrate writeref from using the ruby implementation to go
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1008
  Contributed by johncai
- Switch from honnef.co/go/tools/megacheck to staticcheck
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1021
- Add distributed tracing support with LabKit
  https://gitlab.com/gitlab-org/gitaly/merge_requests/976
- Simplify error wrapping in service/ref
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1009
- Remove dummy RequestStore
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1007
- Simplify error handling in ssh package
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1029
- Add response chunker abstraction
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1031
- Use go implementation of FindCommits
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1025
- Rewrite get commit message
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1012
- Update docs about monitoring and README
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1016
  Contributed by Takuya Noguchi
- Remove unused Ruby rebase/squash code
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1033

## v1.12.2

#### Security
- Disable git protocol v2 temporarily
  https://gitlab.com/gitlab-org/gitaly/merge_requests/

## v1.12.1

#### Fixed
- Fix flaky rebase test
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1028
- Fix regression for https_proxy and unix socket connections
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1032

## v1.12.0

#### Fixed
- Fix wildcard protected branches not working with remote mirrors
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1006

## v1.11.0

#### Fixed
- Fix incorrect tree entries retrieved with directories that have curly braces
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1013
- Deduplicate CA in gitaly tls
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1005

## v1.10.0

#### Added
- Allow repositories to be reduplicated
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1003

#### Fixed
- Add GIT_DIR to hook environment
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1001

#### Performance
- Re-implemented FindBranch in Go
  https://gitlab.com/gitlab-org/gitaly/merge_requests/981

## v1.9.0

#### Changed
- Improve Linking and Unlink object pools RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1000

#### Other
- Fix tests failing due to test-repo change
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1004

## v1.8.0

#### Other
- Log correlation_id field in structured logging output
  https://gitlab.com/gitlab-org/gitaly/merge_requests/995
- Add explicit null byte check in internal/command.New
  https://gitlab.com/gitlab-org/gitaly/merge_requests/997
- README cleanup
  https://gitlab.com/gitlab-org/gitaly/merge_requests/996

## v1.7.2

#### Other
- Fix tests failing due to test-repo change
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1004

#### Security
- Disable git protocol v2 temporarily
  https://gitlab.com/gitlab-org/gitaly/merge_requests/

## v1.7.1

#### Other
- Log correlation_id field in structured logging output
  https://gitlab.com/gitlab-org/gitaly/merge_requests/995

## v1.7.0

#### Added
- Add an RPC that allows repository size to be reduced by bulk-removing internal references
  https://gitlab.com/gitlab-org/gitaly/merge_requests/990

## v1.6.0

#### Other
- Clean up invalid keep-around refs when performing housekeeping
  https://gitlab.com/gitlab-org/gitaly/merge_requests/992

## v1.5.0

#### Added
- Add tls configuration to gitaly golang server
  https://gitlab.com/gitlab-org/gitaly/merge_requests/932

#### Fixed
- Fix TLS client code on macOS
  https://gitlab.com/gitlab-org/gitaly/merge_requests/994

#### Other
- Update to latest goimports formatting
  https://gitlab.com/gitlab-org/gitaly/merge_requests/993

## v1.4.0

#### Added
- Link and Unlink RPCs
  https://gitlab.com/gitlab-org/gitaly/merge_requests/986

## v1.3.0

#### Other
- Remove unused bridge_exceptions method
  https://gitlab.com/gitlab-org/gitaly/merge_requests/987
- Clean up process documentation
  https://gitlab.com/gitlab-org/gitaly/merge_requests/984

## v1.2.0

#### Added
- Upgrade proto to v1.2
  https://gitlab.com/gitlab-org/gitaly/merge_requests/985
- Allow moved files to infer their content based on the source
  https://gitlab.com/gitlab-org/gitaly/merge_requests/980

#### Other
- Add connectivity tests
  https://gitlab.com/gitlab-org/gitaly/merge_requests/968

## v1.1.0

#### Other
- Remove grpc dependency from catfile
  https://gitlab.com/gitlab-org/gitaly/merge_requests/983
- Don't use rugged when calling write-ref
  https://gitlab.com/gitlab-org/gitaly/merge_requests/982

## v1.0.0

#### Added
- Add gitaly-debug production debugging tool
  https://gitlab.com/gitlab-org/gitaly/merge_requests/967

#### Fixed
- Bump gitlab-markup to 1.6.5
  https://gitlab.com/gitlab-org/gitaly/merge_requests/975
- Fix to reallow tcp URLs
  https://gitlab.com/gitlab-org/gitaly/merge_requests/974

#### Other
- Upgrade minimum required Git version to 2.18.0
  https://gitlab.com/gitlab-org/gitaly/merge_requests/958
- Bump tzinfo to 1.2.5
  https://gitlab.com/gitlab-org/gitaly/merge_requests/977
- Bump activesupport gem to 5.0.7
  https://gitlab.com/gitlab-org/gitaly/merge_requests/978
- Propagate correlation-ids in from upstream services and out to Gitaly-Ruby
  https://gitlab.com/gitlab-org/gitaly/merge_requests/970

#### Security
- Bump nokogiri to 1.8.5
  https://gitlab.com/gitlab-org/gitaly/merge_requests/979

## v0.133.0

#### Other
- Upgrade gRPC-go from v1.9.1 to v1.16
  https://gitlab.com/gitlab-org/gitaly/merge_requests/972

## v0.132.0

#### Other
- Upgrade to Ruby 2.5.3
  https://gitlab.com/gitlab-org/gitaly/merge_requests/942
- Remove dead code post 10.8
  https://gitlab.com/gitlab-org/gitaly/merge_requests/964

## v0.131.0

#### Fixed
- Fixed bug with wiki operations enumerator when content nil
  https://gitlab.com/gitlab-org/gitaly/merge_requests/962

## v0.130.0

#### Added
- Support SSH credentials for push mirroring
  https://gitlab.com/gitlab-org/gitaly/merge_requests/959

## v0.129.1

#### Other
- Fix tests failing due to test-repo change
  https://gitlab.com/gitlab-org/gitaly/merge_requests/1004

#### Security
- Disable git protocol v2 temporarily
  https://gitlab.com/gitlab-org/gitaly/merge_requests/

## v0.129.0

#### Added
- Add submodule reference update operation in the repository
  https://gitlab.com/gitlab-org/gitaly/merge_requests/936

#### Fixed
- Improve wiki hook error message
  https://gitlab.com/gitlab-org/gitaly/merge_requests/963
- Fix encoding bug in User{Create,Delete}Tag
  https://gitlab.com/gitlab-org/gitaly/merge_requests/952

#### Other
- Expand Gitlab::Git::Repository unit specs with examples from rails
  https://gitlab.com/gitlab-org/gitaly/merge_requests/945
- Update vendoring
  https://gitlab.com/gitlab-org/gitaly/merge_requests/954

## v0.128.0

#### Fixed
- Fix incorrect committer when committing patches
  https://gitlab.com/gitlab-org/gitaly/merge_requests/947
- Fix makefile 'find ruby/vendor' bug
  https://gitlab.com/gitlab-org/gitaly/merge_requests/946

## v0.127.0

#### Added
- Make git hooks self healing
  https://gitlab.com/gitlab-org/gitaly/merge_requests/886
- Add an endpoint to apply patches to a branch
  https://gitlab.com/gitlab-org/gitaly/merge_requests/926

#### Fixed
- Use $(MAKE) when re-invoking make
  https://gitlab.com/gitlab-org/gitaly/merge_requests/933

#### Other
- Bump google-protobuf gem to 3.6.1
  https://gitlab.com/gitlab-org/gitaly/merge_requests/941
- Bump Rouge gem to 3.3.0
  https://gitlab.com/gitlab-org/gitaly/merge_requests/943
- Upgrade Ruby version to 2.4.5
  https://gitlab.com/gitlab-org/gitaly/merge_requests/944

## v0.126.0

#### Added
- Add support for closing Rugged/libgit2 file descriptors
  https://gitlab.com/gitlab-org/gitaly/merge_requests/903

#### Changed
- Require storage directories to exist at startup
  https://gitlab.com/gitlab-org/gitaly/merge_requests/675

#### Fixed
- Don't confuse govendor license with ruby gem .go files
  https://gitlab.com/gitlab-org/gitaly/merge_requests/935
- Rspec and bundler setup fixes
  https://gitlab.com/gitlab-org/gitaly/merge_requests/901
- Fix git protocol prometheus metrics
  https://gitlab.com/gitlab-org/gitaly/merge_requests/908
- Fix order in config.toml.example
  https://gitlab.com/gitlab-org/gitaly/merge_requests/923

#### Other
- Standardize git command invocation
  https://gitlab.com/gitlab-org/gitaly/merge_requests/915
- Update grpc to v1.15.x in gitaly-ruby
  https://gitlab.com/gitlab-org/gitaly/merge_requests/918
- Add package tests for internal/git/pktline
  https://gitlab.com/gitlab-org/gitaly/merge_requests/909
- Make Makefile more predictable by bootstrapping
  https://gitlab.com/gitlab-org/gitaly/merge_requests/913
- Force english output on git commands
  https://gitlab.com/gitlab-org/gitaly/merge_requests/898
- Restore notice check
  https://gitlab.com/gitlab-org/gitaly/merge_requests/902
- Prevent stale packed-refs file when Gitaly is running on top of NFS
  https://gitlab.com/gitlab-org/gitaly/merge_requests/924

#### Performance
- Update Prometheus vendoring
  https://gitlab.com/gitlab-org/gitaly/merge_requests/922
- Free Rugged open file descriptors in gRPC middleware
  https://gitlab.com/gitlab-org/gitaly/merge_requests/911

#### Removed
- Remove deprecated methods
  https://gitlab.com/gitlab-org/gitaly/merge_requests/910

#### Security
- Bump Rugged to 0.27.5 for security fixes
  https://gitlab.com/gitlab-org/gitaly/merge_requests/907

## v0.125.0

#### Added
- Support Git protocol v2
  https://gitlab.com/gitlab-org/gitaly/merge_requests/844

#### Other
- Remove test case that exercises gogit feature flag
  https://gitlab.com/gitlab-org/gitaly/merge_requests/899

## v0.124.0

#### Deprecated
- Remove support for Go 1.9
  https://gitlab.com/gitlab-org/gitaly/merge_requests/

#### Fixed
- Fix panic in git pktline splitter
  https://gitlab.com/gitlab-org/gitaly/merge_requests/893

#### Other
- Rename gitaly proto import to gitalypb
  https://gitlab.com/gitlab-org/gitaly/merge_requests/895

## v0.123.0

#### Added
- Add ListLastCommitsForTree to retrieve the last commits for every entry in the current path
  https://gitlab.com/gitlab-org/gitaly/merge_requests/881

#### Other
- Wait for gitaly to boot in rspec integration tests
  https://gitlab.com/gitlab-org/gitaly/merge_requests/890

## v0.122.0

#### Added
- Implements CHMOD action of UserCommitFiles API
  https://gitlab.com/gitlab-org/gitaly/merge_requests/884
  Contributed by Jacopo Beschi @jacopo-beschi

#### Changed
- Use CommitDiffRequest.MaxPatchBytes instead of hardcoded limit for single diff patches
  https://gitlab.com/gitlab-org/gitaly/merge_requests/880
- Implement new credentials scheme on gitaly-ruby
  https://gitlab.com/gitlab-org/gitaly/merge_requests/873

#### Fixed
- Export HTTP proxy environment variables to Gitaly
  https://gitlab.com/gitlab-org/gitaly/merge_requests/885

#### Security
- Sanitize sentry events' logentry messages
  https://gitlab.com/gitlab-org/gitaly/merge_requests/

## v0.121.0

#### Changed
- CalculateChecksum: Include keep-around and other references in the checksum calculation
  https://gitlab.com/gitlab-org/gitaly/merge_requests/731

#### Other
- Stop vendoring Gitlab::Git
  https://gitlab.com/gitlab-org/gitaly/merge_requests/883

## v0.120.0

#### Added
- Server implementation ListDirectories
  https://gitlab.com/gitlab-org/gitaly/merge_requests/868

#### Changed
- Return old and new modes on RawChanges
  https://gitlab.com/gitlab-org/gitaly/merge_requests/878

#### Other
- Allow server to receive an hmac token with the client timestamp for auth
  https://gitlab.com/gitlab-org/gitaly/merge_requests/872

## v0.119.0

#### Added
- Add server implementation for FindRemoteRootRef
  https://gitlab.com/gitlab-org/gitaly/merge_requests/874

#### Changed
- Allow merge base to receive more than 2 revisions
  https://gitlab.com/gitlab-org/gitaly/merge_requests/869
- Stop vendoring some Gitlab::Git::* classes
  https://gitlab.com/gitlab-org/gitaly/merge_requests/865

#### Fixed
- Support custom_hooks being a symlink
  https://gitlab.com/gitlab-org/gitaly/merge_requests/871
- Prune large patches by default when enforcing limits
  https://gitlab.com/gitlab-org/gitaly/merge_requests/858
- Fix diffs being collapsed unnecessarily
  https://gitlab.com/gitlab-org/gitaly/merge_requests/854
- Remove stale HEAD.lock if it exists
  https://gitlab.com/gitlab-org/gitaly/merge_requests/861
- Fix patch size calculations to not include headers
  https://gitlab.com/gitlab-org/gitaly/merge_requests/859

#### Other
- Vendor Gitlab::Git at c87ca832263
  https://gitlab.com/gitlab-org/gitaly/merge_requests/860
- Bump gitaly-proto to 0.112.0
  https://gitlab.com/gitlab-org/gitaly/merge_requests/857

#### Security
- Bump rugged to 0.27.4 for security fixes
  https://gitlab.com/gitlab-org/gitaly/merge_requests/856
- Update the sanitize gem to at least 4.6.6
  https://gitlab.com/gitlab-org/gitaly/merge_requests/876
- Bump rouge to 3.2.1
  https://gitlab.com/gitlab-org/gitaly/merge_requests/862

## v0.118.0

#### Added
- Add ability to support custom options for git-receive-pack
  https://gitlab.com/gitlab-org/gitaly/merge_requests/834

## v0.117.2

#### Fixed
- Fix diffs being collapsed unnecessarily
  https://gitlab.com/gitlab-org/gitaly/merge_requests/854
- Fix patch size calculations to not include headers
  https://gitlab.com/gitlab-org/gitaly/merge_requests/859
- Prune large patches by default when enforcing limits
  https://gitlab.com/gitlab-org/gitaly/merge_requests/858

## v0.117.1

#### Security
- Bump rouge to 3.2.1
  https://gitlab.com/gitlab-org/gitaly/merge_requests/862
- Bump rugged to 0.27.4 for security fixes
  https://gitlab.com/gitlab-org/gitaly/merge_requests/856

## v0.117.0

#### Performance
- Only load Wiki formatted data upon request
  https://gitlab.com/gitlab-org/gitaly/merge_requests/839

## v0.116.0

#### Added
- Add ListNewBlobs RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/849

## v0.115.0

#### Added
- Implement DiffService.DiffStats RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/808
- Update gitaly-proto to 0.109.0
  https://gitlab.com/gitlab-org/gitaly/merge_requests/843

#### Changed
- Stop vendoring Gitlab::VersionInfo
  https://gitlab.com/gitlab-org/gitaly/merge_requests/840

#### Fixed
- Check errors and fix chunking in ListNewCommits
  https://gitlab.com/gitlab-org/gitaly/merge_requests/852
- Fix reStructuredText not working on Gitaly nodes
  https://gitlab.com/gitlab-org/gitaly/merge_requests/838

#### Other
- Add auth to the config.toml.example file
  https://gitlab.com/gitlab-org/gitaly/merge_requests/851
- Remove the Dockerfile for Danger since the image is now built by https://gitlab.com/gitlab-org/gitlab-build-images
  https://gitlab.com/gitlab-org/gitaly/merge_requests/836
- Vendor Gitlab::Git at 2ca8219a20f16
  https://gitlab.com/gitlab-org/gitaly/merge_requests/841
- Move diff parser test to own package
  https://gitlab.com/gitlab-org/gitaly/merge_requests/837

## v0.114.0

#### Added
- Remove stale config.lock files
  https://gitlab.com/gitlab-org/gitaly/merge_requests/832

#### Fixed
- Handle nil commit in buildLocalBranch
  https://gitlab.com/gitlab-org/gitaly/merge_requests/822
- Handle non-existing branch on UserDeleteBranch
  https://gitlab.com/gitlab-org/gitaly/merge_requests/826
- Handle non-existing tags on UserDeleteTag
  https://gitlab.com/gitlab-org/gitaly/merge_requests/827

#### Other
- Lower gitaly-ruby default max_rss to 200MB
  https://gitlab.com/gitlab-org/gitaly/merge_requests/833
- Vendor gitlab-git at 92802e51
  https://gitlab.com/gitlab-org/gitaly/merge_requests/825
- Bump Linguist version to match Rails
  https://gitlab.com/gitlab-org/gitaly/merge_requests/821
- Stop vendoring gitlab/git/index.rb
  https://gitlab.com/gitlab-org/gitaly/merge_requests/824
- Bump rspec from 3.6.0 to 3.7.0
  https://gitlab.com/gitlab-org/gitaly/merge_requests/830

#### Performance
- Bump nokogiri to 1.8.4 and sanitize to 4.6.6
  https://gitlab.com/gitlab-org/gitaly/merge_requests/831

#### Security
- Update to gitlab-gollum-lib v4.2.7.5 and make Gemfile consistent with GitLab versions
  https://gitlab.com/gitlab-org/gitaly/merge_requests/828

## v0.113.0

#### Added
- Update Git to 2.18.0
  https://gitlab.com/gitlab-org/gitaly/merge_requests/795
- Implement RefService.FindAllRemoteBranches RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/799

#### Fixed
- Fix lines.Sender message chunking
  https://gitlab.com/gitlab-org/gitaly/merge_requests/817
- Fix nil commit author dereference
  https://gitlab.com/gitlab-org/gitaly/merge_requests/800

#### Other
- Vendor gitlab_git at 740ae2d194f3833e224
  https://gitlab.com/gitlab-org/gitaly/merge_requests/819
- Vendor gitlab-git at 49d7f92fd7476b4fb10e44f
  https://gitlab.com/gitlab-org/gitaly/merge_requests/798
- Vendor gitlab_git at 555afe8971c9ab6f9
  https://gitlab.com/gitlab-org/gitaly/merge_requests/803
- Move git/wiki*.rb out of vendor
  https://gitlab.com/gitlab-org/gitaly/merge_requests/804
- Clean up CI matrix
  https://gitlab.com/gitlab-org/gitaly/merge_requests/811
- Stop vendoring some gitlab_git files we don't need
  https://gitlab.com/gitlab-org/gitaly/merge_requests/801
- Vendor gitlab_git at 16b867d8ce6246ad8
  https://gitlab.com/gitlab-org/gitaly/merge_requests/810
- Vendor gitlab-git at e661896b54da82c0327b1
  https://gitlab.com/gitlab-org/gitaly/merge_requests/814
- Catch SIGINT in gitaly-ruby
  https://gitlab.com/gitlab-org/gitaly/merge_requests/818
- Fix diff path logging
  https://gitlab.com/gitlab-org/gitaly/merge_requests/812
- Exclude more gitlab_git files from vendoring
  https://gitlab.com/gitlab-org/gitaly/merge_requests/815
- Improve ListError message
  https://gitlab.com/gitlab-org/gitaly/merge_requests/809

#### Performance
- Add limit parameter for WikiGetAllPagesRequest
  https://gitlab.com/gitlab-org/gitaly/merge_requests/807

#### Removed
- Remove implementation of Notifications::PostReceive
  https://gitlab.com/gitlab-org/gitaly/merge_requests/806

## v0.112.0

#### Fixed
- Translate more ListConflictFiles errors into FailedPrecondition
  https://gitlab.com/gitlab-org/gitaly/merge_requests/797
- Implement fetch keep-around refs in create from bundle
  https://gitlab.com/gitlab-org/gitaly/merge_requests/790
- Remove unnecessary commit size calculations
  https://gitlab.com/gitlab-org/gitaly/merge_requests/791

#### Other
- Add validation for config keys
  https://gitlab.com/gitlab-org/gitaly/merge_requests/788
- Vendor gitlab-git at b14b31b819f0f09d73e001
  https://gitlab.com/gitlab-org/gitaly/merge_requests/792

#### Performance
- Rewrite ListCommitsByOid in Go
  https://gitlab.com/gitlab-org/gitaly/merge_requests/787

## v0.111.3

#### Security
- Update to gitlab-gollum-lib v4.2.7.5 and make Gemfile consistent with GitLab versions
  https://gitlab.com/gitlab-org/gitaly/merge_requests/828

## v0.111.2

#### Fixed
- Handle nil commit in buildLocalBranch
  https://gitlab.com/gitlab-org/gitaly/merge_requests/822

## v0.111.1

#### Fixed
- Fix nil commit author dereference
  https://gitlab.com/gitlab-org/gitaly/merge_requests/800
- Remove unnecessary commit size calculations
  https://gitlab.com/gitlab-org/gitaly/merge_requests/791

## v0.111.0

#### Added
- Implement DeleteConfig and SetConfig
  https://gitlab.com/gitlab-org/gitaly/merge_requests/786
- Add OperationService.UserUpdateBranch RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/778

#### Other
- Vendor gitlab-git at 7e9f46d0dc1ed34d7
  https://gitlab.com/gitlab-org/gitaly/merge_requests/783
- Vendor gitlab-git at bdb64ac0a1396a7624
  https://gitlab.com/gitlab-org/gitaly/merge_requests/784
- Remove unnecessary existence check in AddNamespace
  https://gitlab.com/gitlab-org/gitaly/merge_requests/785

## v0.110.0

#### Added
- Server implementation ListNewCommits
  https://gitlab.com/gitlab-org/gitaly/merge_requests/779

#### Fixed
- Fix encoding bug in UserCommitFiles
  https://gitlab.com/gitlab-org/gitaly/merge_requests/782

#### Other
- Tweak spawn token defaults and add logging
  https://gitlab.com/gitlab-org/gitaly/merge_requests/781

#### Performance
- Use 'git cat-file' to retrieve commits
  https://gitlab.com/gitlab-org/gitaly/merge_requests/771

#### Security
- Sanitize paths when importing repositories
  https://gitlab.com/gitlab-org/gitaly/merge_requests/780

## v0.109.0

#### Added
- Reject nested storage paths
  https://gitlab.com/gitlab-org/gitaly/merge_requests/773

#### Fixed
- Bump rugged to 0.27.2
  https://gitlab.com/gitlab-org/gitaly/merge_requests/769
- Fix TreeEntry relative path bug
  https://gitlab.com/gitlab-org/gitaly/merge_requests/776

#### Other
- Vendor Gitlab::Git at 292cf668
  https://gitlab.com/gitlab-org/gitaly/merge_requests/777
- Vendor Gitlab::Git at f7b59b9f14
  https://gitlab.com/gitlab-org/gitaly/merge_requests/768
- Vendor Gitlab::Git at 7c11ed8c
  https://gitlab.com/gitlab-org/gitaly/merge_requests/770

## v0.108.0

#### Added
- Server info performs read and write checks
  https://gitlab.com/gitlab-org/gitaly/merge_requests/767

#### Changed
- Remove GoGit
  https://gitlab.com/gitlab-org/gitaly/merge_requests/764

#### Other
- Use custom log levels for grpc-go
  https://gitlab.com/gitlab-org/gitaly/merge_requests/765
- Vendor Gitlab::Git at 2a82179e102159b8416f4a20d3349ef208c58738
  https://gitlab.com/gitlab-org/gitaly/merge_requests/766

## v0.107.0

#### Added
- Add BackupCustomHooks
  https://gitlab.com/gitlab-org/gitaly/merge_requests/760

#### Other
- Try to fix flaky rubyserver.TestRemovals test
  https://gitlab.com/gitlab-org/gitaly/merge_requests/759
- Vendor gitlab_git at a20d3ff2b004e8ab62c037
  https://gitlab.com/gitlab-org/gitaly/merge_requests/761
- Bumping gitlab-gollum-rugged-adapter to version 0.4.4.1 and gitlab-gollum-lib to 4.2.7.4
  https://gitlab.com/gitlab-org/gitaly/merge_requests/762

## v0.106.0

#### Changed
- Colons are not allowed in refs
  https://gitlab.com/gitlab-org/gitaly/merge_requests/747

#### Fixed
- Reraise UnsupportedEncodingError as FailedPrecondition
  https://gitlab.com/gitlab-org/gitaly/merge_requests/718

#### Other
- Vendor gitlab_git at 930ad88a87b0814173989
  https://gitlab.com/gitlab-org/gitaly/merge_requests/752
- Upgrade vendor to d2aa3e3d5fae1017373cc047a9403cfa111b2031
  https://gitlab.com/gitlab-org/gitaly/merge_requests/755

## v0.105.1

#### Other
- Bumping gitlab-gollum-rugged-adapter to version 0.4.4.1 and gitlab-gollum-lib to 4.2.7.4
  https://gitlab.com/gitlab-org/gitaly/merge_requests/762

## v0.105.0

#### Added
- RestoreCustomHooks
  https://gitlab.com/gitlab-org/gitaly/merge_requests/741

#### Changed
- Rewrite Repository::Fsck in Go
  https://gitlab.com/gitlab-org/gitaly/merge_requests/738

#### Fixed
- Fix committer bug in go-git adapter
  https://gitlab.com/gitlab-org/gitaly/merge_requests/748

## v0.104.0

#### Added
- Use Go-Git for the FindCommit RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/691

#### Fixed
- Ignore ENOENT when cleaning up lock files
  https://gitlab.com/gitlab-org/gitaly/merge_requests/740
- Fix rename similarity in CommitDiff
  https://gitlab.com/gitlab-org/gitaly/merge_requests/727
- Use grpc 1.11.0 in gitaly-ruby
  https://gitlab.com/gitlab-org/gitaly/merge_requests/732

#### Other
- Tests: only match error strings we create
  https://gitlab.com/gitlab-org/gitaly/merge_requests/743
- Use gitaly-proto 0.101.0
  https://gitlab.com/gitlab-org/gitaly/merge_requests/745
- Upgrade to Ruby 2.4.4
  https://gitlab.com/gitlab-org/gitaly/merge_requests/725
- Use the same faraday gem version as gitlab-ce
  https://gitlab.com/gitlab-org/gitaly/merge_requests/733

#### Performance
- Rewrite IsRebase/SquashInProgress in Go
  https://gitlab.com/gitlab-org/gitaly/merge_requests/698

#### Security
- Use rugged 0.27.1 for security fixes
  https://gitlab.com/gitlab-org/gitaly/merge_requests/744

## v0.103.0

#### Added
- Add StorageService::DeleteAllRepositories RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/726

#### Other
- Fix Dangerfile bad changelog detection
  https://gitlab.com/gitlab-org/gitaly/merge_requests/724

## v0.102.0

#### Changed
- Unvendor Repository#add_branch
  https://gitlab.com/gitlab-org/gitaly/merge_requests/717

#### Fixed
- Fix matching bug in SearchFilesByContent
  https://gitlab.com/gitlab-org/gitaly/merge_requests/722

## v0.101.0

#### Changed
- Add gitaly-ruby installation debug log messages
  https://gitlab.com/gitlab-org/gitaly/merge_requests/710

#### Fixed
- Use round robin load balancing instead of 'pick first' for gitaly-ruby
  https://gitlab.com/gitlab-org/gitaly/merge_requests/700

#### Other
- Generate changelog when releasing a tag to prevent merge conflicts
  https://gitlab.com/gitlab-org/gitaly/merge_requests/719
- Unvendor Repository#create implementation
  https://gitlab.com/gitlab-org/gitaly/merge_requests/713

## v0.100.0

- Fix WikiFindPage when the page has invalidly-encoded content
  https://gitlab.com/gitlab-org/gitaly/merge_requests/712
- Add danger container to the Gitaly project
  https://gitlab.com/gitlab-org/gitaly/merge_requests/711
- Remove ruby concurrency limiter
  https://gitlab.com/gitlab-org/gitaly/merge_requests/708
- Drop support for Golang 1.8
  https://gitlab.com/gitlab-org/gitaly/merge_requests/715
- Introduce src-d/go-git as dependency
  https://gitlab.com/gitlab-org/gitaly/merge_requests/709
- Lower spawn log level to 'debug'
  https://gitlab.com/gitlab-org/gitaly/merge_requests/714

## v0.99.0

- Improve changelog entry checks using Danger
  https://gitlab.com/gitlab-org/gitaly/merge_requests/705
- GetBlobs: don't create blob reader if limit is zero
  https://gitlab.com/gitlab-org/gitaly/merge_requests/706
- Implement SearchFilesBy{Content,Name}
  https://gitlab.com/gitlab-org/gitaly/merge_requests/677
- Introduce feature flag package based on gRPC metadata
  https://gitlab.com/gitlab-org/gitaly/merge_requests/704
- Return DataLoss error for non-valid git repositories when calculating the checksum
  https://gitlab.com/gitlab-org/gitaly/merge_requests/697

## v0.98.0

- Server implementation for repository raw_changes
  https://gitlab.com/gitlab-org/gitaly/merge_requests/699
- Add 'large request' test case to ListCommitsByOid
  https://gitlab.com/gitlab-org/gitaly/merge_requests/703
- Vendor gitlab_git at gitlab-org/gitlab-ce@3fcb9c115d776feb
  https://gitlab.com/gitlab-org/gitaly/merge_requests/702
- Limit concurrent gitaly-ruby requests from the client side
  https://gitlab.com/gitlab-org/gitaly/merge_requests/695
- Allow configuration of the log level in `config.toml`
  https://gitlab.com/gitlab-org/gitaly/merge_requests/696
- Copy Gitlab::Git::Repository#exists? implementation for internal method calls
  https://gitlab.com/gitlab-org/gitaly/merge_requests/693
- Upgrade Licensee gem to match the CE gem
  https://gitlab.com/gitlab-org/gitaly/merge_requests/693
- Vendor gitlab_git at 8b41c40674273d6ee
  https://gitlab.com/gitlab-org/gitaly/merge_requests/684
- Make wiki commit fields backwards compatible
  https://gitlab.com/gitlab-org/gitaly/merge_requests/685
- Catch CommitErrors while rebasing
  https://gitlab.com/gitlab-org/gitaly/merge_requests/680

## v0.97.0

- Use gitaly-proto 0.97.0
  https://gitlab.com/gitlab-org/gitaly/merge_requests/683
- Make gitaly-ruby's grpc server log at level WARN
  https://gitlab.com/gitlab-org/gitaly/merge_requests/681
- Add health checks for gitaly-ruby
  https://gitlab.com/gitlab-org/gitaly/merge_requests/678
- Add config option to point to languages.json
  https://gitlab.com/gitlab-org/gitaly/merge_requests/652

## v0.96.1

- Vendor gitlab_git at 7e3bb679a92156304
  https://gitlab.com/gitlab-org/gitaly/merge_requests/669
- Make it a fatal error if gitaly-ruby can't start
  https://gitlab.com/gitlab-org/gitaly/merge_requests/667
- Tag log entries with repo.GlRepository
  https://gitlab.com/gitlab-org/gitaly/merge_requests/663
- Add {Get,CreateRepositoryFrom}Snapshot RPCs
  https://gitlab.com/gitlab-org/gitaly/merge_requests/644

## v0.96.0

Skipped. We cut and pushed the wrong tag.

## v0.95.0
- Fix fragile checksum test
  https://gitlab.com/gitlab-org/gitaly/merge_requests/661
- Use rugged 0.27.0
  https://gitlab.com/gitlab-org/gitaly/merge_requests/660

## v0.94.0

- Send gitaly-ruby exceptions to their own DSN
  https://gitlab.com/gitlab-org/gitaly/merge_requests/656
- Run Go test suite with '-race' in CI
  https://gitlab.com/gitlab-org/gitaly/merge_requests/654
- Ignore more grpc codes in sentry
  https://gitlab.com/gitlab-org/gitaly/merge_requests/655
- Implement Get{Tag,Commit}Messages RPCs
  https://gitlab.com/gitlab-org/gitaly/merge_requests/646
- Fix directory permission walker for Go 1.10
  https://gitlab.com/gitlab-org/gitaly/merge_requests/650

## v0.93.0

- Fix concurrency limit handler stream interceptor
  https://gitlab.com/gitlab-org/gitaly/merge_requests/640
- Vendor gitlab_git at 9b76d8512a5491202e5a953
  https://gitlab.com/gitlab-org/gitaly/merge_requests/647
- Add handling for large commit and tag messages
  https://gitlab.com/gitlab-org/gitaly/merge_requests/635
- Update gitaly-proto to v0.91.0
  https://gitlab.com/gitlab-org/gitaly/merge_requests/643

## v0.92.0

- Server Implementation GetInfoAttributes
  https://gitlab.com/gitlab-org/gitaly/merge_requests/641
- Fix encoding error in ListConflictFiles
  https://gitlab.com/gitlab-org/gitaly/merge_requests/639
- Add catfile convenience methods
  https://gitlab.com/gitlab-org/gitaly/merge_requests/638
- Server implementation FindRemoteRepository
  https://gitlab.com/gitlab-org/gitaly/merge_requests/636
- Log process PID in 'spawn complete' entry
  https://gitlab.com/gitlab-org/gitaly/merge_requests/637
- Vendor gitlab_git at 79aa00321063da
  https://gitlab.com/gitlab-org/gitaly/merge_requests/633

## v0.91.0

- Rewrite RepositoryService.HasLocalBranches in Go
  https://gitlab.com/gitlab-org/gitaly/merge_requests/629
- Rewrite RepositoryService.MergeBase in Go
  https://gitlab.com/gitlab-org/gitaly/merge_requests/632
- Encode OperationsService errors in UTF-8 before sending them
  https://gitlab.com/gitlab-org/gitaly/merge_requests/627
- Add param logging in NamespaceService RPCs
  https://gitlab.com/gitlab-org/gitaly/merge_requests/626
- Sanitize URLs before sending gitaly-ruby exceptions to Sentry
  https://gitlab.com/gitlab-org/gitaly/merge_requests/625

## v0.90.0

- Implement SSHService.SSHUploadArchive RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/621
- Sanitize URLs before logging them
  https://gitlab.com/gitlab-org/gitaly/merge_requests/624
- Clean stale worktrees before performing garbage collection
  https://gitlab.com/gitlab-org/gitaly/merge_requests/622

## v0.89.0

- Report original exceptions to Sentry instead of wrapped ones by the exception bridge
  https://gitlab.com/gitlab-org/gitaly/merge_requests/623
- Upgrade grpc gem to 1.10.0
  https://gitlab.com/gitlab-org/gitaly/merge_requests/620
- Fix FetchRemote throwing "Host key verification failed"
  https://gitlab.com/gitlab-org/gitaly/merge_requests/617
- Use only 1 gitaly-ruby process in test
  https://gitlab.com/gitlab-org/gitaly/merge_requests/615
- Bump github-linguist to 5.3.3
  https://gitlab.com/gitlab-org/gitaly/merge_requests/613

## v0.88.0

- Add support for all field to {Find,Count}Commits RPCs
  https://gitlab.com/gitlab-org/gitaly/merge_requests/611
- Vendor gitlab_git at de454de9b10f
  https://gitlab.com/gitlab-org/gitaly/merge_requests/611

## v0.87.0

- Implement GetCommitSignatures RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/609

## v0.86.0

- Implement BlobService.GetAllLfsPointers
  https://gitlab.com/gitlab-org/gitaly/merge_requests/562
- Implement BlobService.GetNewLfsPointers
  https://gitlab.com/gitlab-org/gitaly/merge_requests/562
- Use gitaly-proto v0.86.0
  https://gitlab.com/gitlab-org/gitaly/merge_requests/606

## v0.85.0

- Implement recursive tree entries fetching
  https://gitlab.com/gitlab-org/gitaly/merge_requests/600

## v0.84.0

- Send gitaly-ruby exceptions to Sentry
  https://gitlab.com/gitlab-org/gitaly/merge_requests/598
- Detect License type for repositories
  https://gitlab.com/gitlab-org/gitaly/merge_requests/601

## v0.83.0

- Delete old lock files before performing Garbage Collection
  https://gitlab.com/gitlab-org/gitaly/merge_requests/587

## v0.82.0

- Implement RepositoryService.IsSquashInProgress RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/593
- Added test to prevent wiki page duplication
  https://gitlab.com/gitlab-org/gitaly/merge_requests/539
- Fixed bug in wiki_find_page method
  https://gitlab.com/gitlab-org/gitaly/merge_requests/590

## v0.81.0

- Vendor gitlab_git at 7095c2bf4064911
  https://gitlab.com/gitlab-org/gitaly/merge_requests/591
- Vendor gitlab_git at 9483cbab26ad239
  https://gitlab.com/gitlab-org/gitaly/merge_requests/588

## v0.80.0

- Lock protobuf to 3.5.1
  https://gitlab.com/gitlab-org/gitaly/merge_requests/589

## v0.79.0

- Update the activesupport gem
  https://gitlab.com/gitlab-org/gitaly/merge_requests/584
- Update the grpc gem to 1.8.7
  https://gitlab.com/gitlab-org/gitaly/merge_requests/585
- Implement GetBlobs RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/582
- Check info split size in catfile parser
  https://gitlab.com/gitlab-org/gitaly/merge_requests/583

## v0.78.0

- Vendor gitlab_git at 498d32363aa61d679ff749b
  https://gitlab.com/gitlab-org/gitaly/merge_requests/579
- Convert inputs to UTF-8 before passing them to Gollum
  https://gitlab.com/gitlab-org/gitaly/merge_requests/575
- Implement OperationService.UserSquash RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/548
- Update recommended and minimum git versions to 2.14.3 and 2.9.0 respectively
  https://gitlab.com/gitlab-org/gitaly/merge_requests/548
- Handle binary commit messages better
  https://gitlab.com/gitlab-org/gitaly/merge_requests/577
- Vendor gitlab_git at a03ea19332736c36ecb9
  https://gitlab.com/gitlab-org/gitaly/merge_requests/574

## v0.77.0

- Implement RepositoryService.WriteConfig RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/554

## v0.76.0

- Add support for PreReceiveError in UserMergeBranch
  https://gitlab.com/gitlab-org/gitaly/merge_requests/573
- Add support for Refs field in DeleteRefs RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/565
- Wait between ruby worker removal from pool and graceful shutdown
  https://gitlab.com/gitlab-org/gitaly/merge_requests/567
- Register the ServerService
  https://gitlab.com/gitlab-org/gitaly/merge_requests/572
- Vendor gitlab_git at f8dd398a21b19cb7d56
  https://gitlab.com/gitlab-org/gitaly/merge_requests/571
- Vendor gitlab_git at 4376be84ce18cde22febc50
  https://gitlab.com/gitlab-org/gitaly/merge_requests/570

## v0.75.0

- Implement WikiGetFormattedData RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/564
- Implement ServerVersion and ServerGitVersion
  https://gitlab.com/gitlab-org/gitaly/merge_requests/561
- Vendor Gitlab::Git @ f9b946c1c9756533fd95c8735803d7b54d6dd204
  https://gitlab.com/gitlab-org/gitaly/merge_requests/563
- ListBranchNamesContainingCommit server implementation
  https://gitlab.com/gitlab-org/gitaly/merge_requests/537
- ListTagNamesContainingCommit server implementation
  https://gitlab.com/gitlab-org/gitaly/merge_requests/537

## v0.74.0

- Implement CreateRepositoryFromBundle RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/557
- Use gitaly-proto v0.77.0
  https://gitlab.com/gitlab-org/gitaly/merge_requests/556
- Automatically remove tempdir when context is over
  https://gitlab.com/gitlab-org/gitaly/merge_requests/555
- Add automatic tempdir cleaner
  https://gitlab.com/gitlab-org/gitaly/merge_requests/540

## v0.73.0

- Implement CreateBundle RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/546

## v0.72.0

- Implement RemoteService.UpdateRemoteMirror RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/544
- Implement OperationService.UserCommitFiles RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/516
- Use grpc-go 1.9.1
  https://gitlab.com/gitlab-org/gitaly/merge_requests/547

## v0.71.0

- Implement GetLfsPointers RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/543
- Add tempdir package
  https://gitlab.com/gitlab-org/gitaly/merge_requests/538
- Fix validation for Repositoryservice::WriteRef
  https://gitlab.com/gitlab-org/gitaly/merge_requests/542

## v0.70.0

- Handle non-existent commits in ExtractCommitSignature
  https://gitlab.com/gitlab-org/gitaly/merge_requests/535
- Implement RepositoryService::WriteRef
  https://gitlab.com/gitlab-org/gitaly/merge_requests/526

## v0.69.0

- Fix handling of paths ending with slashes in TreeEntry
  https://gitlab.com/gitlab-org/gitaly/merge_requests/532
- Implement CreateRepositoryFromURL RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/529

## v0.68.0

- Check repo existence before passing to gitaly-ruby
  https://gitlab.com/gitlab-org/gitaly/merge_requests/528
- Implement ExtractCommitSignature RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/521
- Update Gitlab::Git vendoring to b10ea6e386a025759aca5e9ef0d23931e77d1012
  https://gitlab.com/gitlab-org/gitaly/merge_requests/525
- Use gitlay-proto 0.71.0
  https://gitlab.com/gitlab-org/gitaly/merge_requests/524
- Fix pagination bug in GetWikiPageVersions
  https://gitlab.com/gitlab-org/gitaly/merge_requests/524
- Use gitaly-proto 0.70.0
  https://gitlab.com/gitlab-org/gitaly/merge_requests/522

## v0.67.0

- Implement UserRebase RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/511
- Implement IsRebaseInProgress RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/519
- Update to gitaly-proto v0.67.0
  https://gitlab.com/gitlab-org/gitaly/merge_requests/520
- Fix an error in merged all branches logic
  https://gitlab.com/gitlab-org/gitaly/merge_requests/517
- Allow RemoteService.AddRemote to receive several mirror_refmaps
  https://gitlab.com/gitlab-org/gitaly/merge_requests/513
- Update vendored gitlab_git to 33cea50976
  https://gitlab.com/gitlab-org/gitaly/merge_requests/518
- Update vendored gitlab_git to bce886b776a
  https://gitlab.com/gitlab-org/gitaly/merge_requests/515
- Update vendored gitlab_git to 6eeb69fc9a2
  https://gitlab.com/gitlab-org/gitaly/merge_requests/514
- Add support for MergedBranches in FindAllBranches RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/510

## v0.66.0

- Implement RemoteService.FetchInternalRemote RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/508

## v0.65.0

- Add support for MaxCount in CountCommits RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/507
- Implement CreateFork RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/497

## v0.64.0

- Update vendored gitlab_git to b98c69470f52185117fcdb5e28096826b32363ca
  https://gitlab.com/gitlab-org/gitaly/merge_requests/506

## v0.63.0

- Handle failed merge when branch gets updated
  https://gitlab.com/gitlab-org/gitaly/merge_requests/505

## v0.62.0

- Implement ConflictsService.ResolveConflicts RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/470
- Implement ConflictsService.ListConflictFiles RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/470
- Implement RemoteService.RemoveRemote RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/490
- Implement RemoteService.AddRemote RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/490

## v0.61.1

- gitaly-ruby shutdown improvements
  https://gitlab.com/gitlab-org/gitaly/merge_requests/500
- Use go 1.9
  https://gitlab.com/gitlab-org/gitaly/merge_requests/496

## v0.61.0

- Add rdoc to gitaly-ruby's Gemfile
  https://gitlab.com/gitlab-org/gitaly/merge_requests/487
- Limit the number of concurrent process spawns
  https://gitlab.com/gitlab-org/gitaly/merge_requests/492
- Update vendored gitlab_git to 858edadf781c0cc54b15832239c19fca378518ad
  https://gitlab.com/gitlab-org/gitaly/merge_requests/493
- Eagerly close logrus writer pipes
  https://gitlab.com/gitlab-org/gitaly/merge_requests/489
- Panic if a command has no Done() channel
  https://gitlab.com/gitlab-org/gitaly/merge_requests/485
- Update vendored gitlab_git to 31fa9313991881258b4697cb507cfc8ab205b7dc
  https://gitlab.com/gitlab-org/gitaly/merge_requests/486

## v0.60.0

- Implement FindMergeBase RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/477
- Update vendored gitlab_git to 359b65beac43e009b715c2db048e06b6f96b0ee8
  https://gitlab.com/gitlab-org/gitaly/merge_requests/481

## v0.59.0

- Restart gitaly-ruby when it uses too much memory
  https://gitlab.com/gitlab-org/gitaly/merge_requests/465

## v0.58.0

- Implement RepostoryService::Fsck
  https://gitlab.com/gitlab-org/gitaly/merge_requests/475
- Increase default gitaly-ruby connection timeout to 40s
  https://gitlab.com/gitlab-org/gitaly/merge_requests/476
- Update vendored gitlab_git to f3a3bd50eafdcfcaeea21d6cfa0b8bbae7720fec
  https://gitlab.com/gitlab-org/gitaly/merge_requests/478

## v0.57.0

- Implement UserRevert RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/471
- Fix commit message encoding and support alternates in CatFile
  https://gitlab.com/gitlab-org/gitaly/merge_requests/469
- Raise an exception when Git::Env.all is called
  https://gitlab.com/gitlab-org/gitaly/merge_requests/474
- Update vendored gitlab_git to c594659fea15c6dd17b
  https://gitlab.com/gitlab-org/gitaly/merge_requests/473
- More logging in housekeeping
  https://gitlab.com/gitlab-org/gitaly/merge_requests/435

## v0.56.0

- Implement UserCherryPick RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/457
- Use grpc-go 1.8.0
  https://gitlab.com/gitlab-org/gitaly/merge_requests/466
- Fix a panic in ListFiles RPC when git process is killed abruptly
  https://gitlab.com/gitlab-org/gitaly/merge_requests/460
- Implement CommitService::FilterShasWithSignatures
  https://gitlab.com/gitlab-org/gitaly/merge_requests/461
- Implement CommitService::ListCommitsByOid
  https://gitlab.com/gitlab-org/gitaly/merge_requests/438

## v0.55.0

- Include pprof debug access in the Prometheus listener
  https://gitlab.com/gitlab-org/gitaly/merge_requests/442
- Run gitaly-ruby in the same directory as gitaly
  https://gitlab.com/gitlab-org/gitaly/merge_requests/458

## v0.54.0

- Implement RefService.DeleteRefs
  https://gitlab.com/gitlab-org/gitaly/merge_requests/453
- Use --deployment flag for bundler and force `bundle install` on `make assemble`
  https://gitlab.com/gitlab-org/gitaly/merge_requests/448
- Update License as requested in: gitlab-com/organization#146
- Implement RepositoryService::FetchSourceBranch
  https://gitlab.com/gitlab-org/gitaly/merge_requests/434

## v0.53.0

- Update vendored gitlab_git to f7537ce03a29b
  https://gitlab.com/gitlab-org/gitaly/merge_requests/449
- Update vendored gitlab_git to 6f045671e665e42c7
  https://gitlab.com/gitlab-org/gitaly/merge_requests/446
- Implement WikiGetPageVersions RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/430

## v0.52.1

- Include pprof debug access in the Prometheus listener
  https://gitlab.com/gitlab-org/gitaly/merge_requests/442

## v0.52.0

- Implement WikiUpdatePage RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/422

## v0.51.0

- Implement OperationService.UserFFMerge
  https://gitlab.com/gitlab-org/gitaly/merge_requests/426
- Implement WikiFindFile RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/425
- Implement WikiDeletePage RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/414
- Implement WikiFindPage RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/419
- Update gitlab_git to b3ba3996e0bd329eaa574ff53c69673efaca6eef and set
  `GL_USERNAME` env variable for hook excecution
  https://gitlab.com/gitlab-org/gitaly/merge_requests/423
- Enable logging in client-streamed and bidi GRPC requests
  https://gitlab.com/gitlab-org/gitaly/merge_requests/429

## v0.50.0

- Pass repo git alternate dirs to gitaly-ruby
  https://gitlab.com/gitlab-org/gitaly/merge_requests/421
- Remove old temporary files from repositories after GC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/411

## v0.49.0

- Use sentry fingerprinting to group exceptions
  https://gitlab.com/gitlab-org/gitaly/merge_requests/417
- Use gitlab_git c23c09366db610c1
  https://gitlab.com/gitlab-org/gitaly/merge_requests/415

## v0.48.0

- Implement WikiWritePage RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/410

## v0.47.0

- Pass full BranchUpdate result on successful merge
  https://gitlab.com/gitlab-org/gitaly/merge_requests/406
- Deprecate implementation of RepositoryService.Exists
  https://gitlab.com/gitlab-org/gitaly/merge_requests/408
- Use gitaly-proto 0.42.0
  https://gitlab.com/gitlab-org/gitaly/merge_requests/407


## v0.46.0

- Add a Rails logger to ruby-git
  https://gitlab.com/gitlab-org/gitaly/merge_requests/405
- Add `git version` to `gitaly_build_info` metrics
  https://gitlab.com/gitlab-org/gitaly/merge_requests/400
- Use relative paths for git object dir attributes
  https://gitlab.com/gitlab-org/gitaly/merge_requests/393

## v0.45.1

- Implement OperationService::UserMergeBranch
  https://gitlab.com/gitlab-org/gitaly/merge_requests/394
- Add client feature logging and metrics
  https://gitlab.com/gitlab-org/gitaly/merge_requests/392
- Implement RepositoryService.HasLocalBranches RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/397
- Fix Commit Subject parsing in rubyserver
  https://gitlab.com/gitlab-org/gitaly/merge_requests/388

## v0.45.0

Skipped. We cut and pushed the wrong tag.

## v0.44.0

- Update gitlab_git to 4a0f720a502ac02423
  https://gitlab.com/gitlab-org/gitaly/merge_requests/389
- Fix incorrect parsing of diff chunks starting with ++ or --
  https://gitlab.com/gitlab-org/gitaly/merge_requests/385
- Implement Raw{Diff,Patch} RPCs
  https://gitlab.com/gitlab-org/gitaly/merge_requests/381

## v0.43.0

- Pass details of Gitaly-Ruby's Ruby exceptions back to
  callers in the request trailers
  https://gitlab.com/gitlab-org/gitaly/merge_requests/358
- Allow individual endpoints to be rate-limited per-repository
  https://gitlab.com/gitlab-org/gitaly/merge_requests/376
- Implement OperationService.UserDeleteBranch RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/377
- Fix path bug in CommitService::FindCommits
  https://gitlab.com/gitlab-org/gitaly/merge_requests/364
- Fail harder during startup, fix version string
  https://gitlab.com/gitlab-org/gitaly/merge_requests/379
- Implement RepositoryService.GetArchive RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/370
- Add `gitaly-ssh` command
  https://gitlab.com/gitlab-org/gitaly/merge_requests/368

## v0.42.0

- Implement UserCreateTag RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/374
- Return pre-receive errors in UserDeleteTag response
  https://gitlab.com/gitlab-org/gitaly/merge_requests/378
- Check if we don't overwrite a namespace moved to gitaly
  https://gitlab.com/gitlab-org/gitaly/merge_requests/375

## v0.41.0

- Wait for monitor goroutine to return during supervisor shutdown
  https://gitlab.com/gitlab-org/gitaly/merge_requests/341
- Use grpc 1.6.0 and update all the things
  https://gitlab.com/gitlab-org/gitaly/merge_requests/354
- Update vendored gitlab_git to 4c6c105909ea610eac7
  https://gitlab.com/gitlab-org/gitaly/merge_requests/360
- Implement UserDeleteTag RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/366
- Implement RepositoryService::CreateRepository
  https://gitlab.com/gitlab-org/gitaly/merge_requests/361
- Fix path bug for gitlab-shell. gitlab-shell path is now required
  https://gitlab.com/gitlab-org/gitaly/merge_requests/365
- Remove support for legacy services not ending in 'Service'
  https://gitlab.com/gitlab-org/gitaly/merge_requests/363
- Implement RepositoryService.UserCreateBranch
  https://gitlab.com/gitlab-org/gitaly/merge_requests/344
- Make gitaly-ruby config mandatory
  https://gitlab.com/gitlab-org/gitaly/merge_requests/373

## v0.40.0
- Use context cancellation instead of command.Close
  https://gitlab.com/gitlab-org/gitaly/merge_requests/332
- Fix LastCommitForPath handling of tree root
  https://gitlab.com/gitlab-org/gitaly/merge_requests/350
- Don't use 'bundle show' to find Linguist
  https://gitlab.com/gitlab-org/gitaly/merge_requests/339
- Fix diff parsing when the last 10 bytes of a stream contain newlines
  https://gitlab.com/gitlab-org/gitaly/merge_requests/348
- Consume diff binary notice as a patch
  https://gitlab.com/gitlab-org/gitaly/merge_requests/349
- Handle git dates larger than golang's and protobuf's limits
  https://gitlab.com/gitlab-org/gitaly/merge_requests/353

## v0.39.0
- Reimplement FindAllTags RPC in Ruby
  https://gitlab.com/gitlab-org/gitaly/merge_requests/334
- Re-use gitaly-ruby client connection
  https://gitlab.com/gitlab-org/gitaly/merge_requests/330
- Fix encoding-bug in GitalyServer#gitaly_commit_from_rugged
  https://gitlab.com/gitlab-org/gitaly/merge_requests/337

## v0.38.0

- Update vendor/gitlab_git to b58c4f436abaf646703bdd80f266fa4c0bab2dd2
  https://gitlab.com/gitlab-org/gitaly/merge_requests/324
- Add missing cmd.Close in log.GetCommit
  https://gitlab.com/gitlab-org/gitaly/merge_requests/326
- Populate `flat_path` field of `TreeEntry`s
  https://gitlab.com/gitlab-org/gitaly/merge_requests/328

## v0.37.0

- Implement FindBranch RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/315

## v0.36.0

- Terminate commands when their context cancels
  https://gitlab.com/gitlab-org/gitaly/merge_requests/318
- Implement {Create,Delete}Branch RPCs
  https://gitlab.com/gitlab-org/gitaly/merge_requests/311
- Use git-linguist to implement CommitLanguages
  https://gitlab.com/gitlab-org/gitaly/merge_requests/316

## v0.35.0

- Implement CommitService.CommitStats
  https://gitlab.com/gitlab-org/gitaly/merge_requests/312
- Use bufio.Reader instead of bufio.Scanner for lines.Send
  https://gitlab.com/gitlab-org/gitaly/merge_requests/303
- Restore support for custom environment variables
  https://gitlab.com/gitlab-org/gitaly/merge_requests/319

## v0.34.0

- Export environment variables for git debugging
  https://gitlab.com/gitlab-org/gitaly/merge_requests/306
- Fix bugs in RepositoryService.FetchRemote
  https://gitlab.com/gitlab-org/gitaly/merge_requests/300
- Respawn gitaly-ruby when it crashes
  https://gitlab.com/gitlab-org/gitaly/merge_requests/293
- Use a fixed order when auto-loading Ruby files
  https://gitlab.com/gitlab-org/gitaly/merge_requests/302
- Add signal handler for ruby socket cleanup on shutdown
  https://gitlab.com/gitlab-org/gitaly/merge_requests/304
- Use grpc 1.4.5 in gitaly-ruby
  https://gitlab.com/gitlab-org/gitaly/merge_requests/308
- Monitor gitaly-ruby RSS via Prometheus
  https://gitlab.com/gitlab-org/gitaly/merge_requests/310

## v0.33.0

- Implement DiffService.CommitPatch RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/279
- Use 'bundle config' for gitaly-ruby in source production installations
  https://gitlab.com/gitlab-org/gitaly/merge_requests/298

## v0.32.0

- RefService::RefExists endpoint
  https://gitlab.com/gitlab-org/gitaly/merge_requests/275

## v0.31.0

- Implement CommitService.FindCommits
  https://gitlab.com/gitlab-org/gitaly/merge_requests/266
- Log spawned process metrics
  https://gitlab.com/gitlab-org/gitaly/merge_requests/284
- Implement RepositoryService.ApplyGitattributes RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/278
- Implement RepositoryService.FetchRemote RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/276

## v0.30.0

- Add a middleware for handling Git object dir attributes
  https://gitlab.com/gitlab-org/gitaly/merge_requests/273

## v0.29.0

- Use BUNDLE_PATH instead of --path for gitaly-ruby
  https://gitlab.com/gitlab-org/gitaly/merge_requests/271
- Add GitLab-Shell Path to config
  https://gitlab.com/gitlab-org/gitaly/merge_requests/267
- Don't count on PID 1 to be the reaper
  https://gitlab.com/gitlab-org/gitaly/merge_requests/270
- Log top level project group for easier analysis
  https://gitlab.com/gitlab-org/gitaly/merge_requests/272

## v0.28.0

- Increase gitaly-ruby connection timeout to 20s
  https://gitlab.com/gitlab-org/gitaly/merge_requests/265
- Implement RepositorySize RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/262
- Implement CommitsByMessage RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/263

## v0.27.0

- Support `git -c` options in SSH upload-pack
  https://gitlab.com/gitlab-org/gitaly/merge_requests/242
- Add storage dir existence check to repo lookup
  https://gitlab.com/gitlab-org/gitaly/merge_requests/259
- Implement RawBlame RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/257
- Implement LastCommitForPath RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/260
- Deprecate Exists RPC in favor of RepositoryExists
  https://gitlab.com/gitlab-org/gitaly/merge_requests/260
- Install gems into vendor/bundle
  https://gitlab.com/gitlab-org/gitaly/merge_requests/264

## v0.26.0

- Implement CommitService.CommitLanguages, add gitaly-ruby
  https://gitlab.com/gitlab-org/gitaly/merge_requests/210
- Extend CountCommits RPC to support before/after/path arguments
  https://gitlab.com/gitlab-org/gitaly/merge_requests/252
- Fix a bug in FindAllTags parsing lightweight tags
  https://gitlab.com/gitlab-org/gitaly/merge_requests/256

## v0.25.0

- Implement FindAllTags RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/246

## v0.24.1

- Return an empty array on field `ParentIds` of `GitCommit`s if it has none
  https://gitlab.com/gitlab-org/gitaly/merge_requests/237

## v0.24.0

- Consume stdout during repack/gc
  https://gitlab.com/gitlab-org/gitaly/merge_requests/249
- Implement RefService.FindAllBranches RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/239

## v0.23.0

- Version without Build Time
  https://gitlab.com/gitlab-org/gitaly/merge_requests/231
- Implement CommitService.ListFiles
  https://gitlab.com/gitlab-org/gitaly/merge_requests/205
- Change the build process from copying to using symlinks
  https://gitlab.com/gitlab-org/gitaly/merge_requests/230
- Implement CommitService.FindCommit
  https://gitlab.com/gitlab-org/gitaly/merge_requests/217
- Register RepositoryService
  https://gitlab.com/gitlab-org/gitaly/merge_requests/233
- Correctly handle a non-tree path on CommitService.TreeEntries
  https://gitlab.com/gitlab-org/gitaly/merge_requests/234

## v0.22.0

- Various build file improvements
  https://gitlab.com/gitlab-org/gitaly/merge_requests/229
- Implement FindAllCommits RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/226
- Send full repository path instead of filename on field `path` of TreeEntry
  https://gitlab.com/gitlab-org/gitaly/merge_requests/232

## v0.21.2

- Config: do not start Gitaly without at least one storage
  https://gitlab.com/gitlab-org/gitaly/merge_requests/227
- Implement CommitService.GarbageCollect/Repack{Incremental,Full}
  https://gitlab.com/gitlab-org/gitaly/merge_requests/218

## v0.21.1

- Make sure stdout.Read has enough bytes buffered to read from
  https://gitlab.com/gitlab-org/gitaly/merge_requests/224

## v0.21.0

- Send an empty response for TreeEntry instead of nil
  https://gitlab.com/gitlab-org/gitaly/merge_requests/223

## v0.20.0

- Implement commit diff limiting logic
  https://gitlab.com/gitlab-org/gitaly/merge_requests/211
- Increase message size to 5 KB for Diff service
  https://gitlab.com/gitlab-org/gitaly/merge_requests/221

## v0.19.0

- Send parent ids and raw body on CommitService.CommitsBetween
  https://gitlab.com/gitlab-org/gitaly/merge_requests/216
- Streamio chunk size optimizations
  https://gitlab.com/gitlab-org/gitaly/merge_requests/206
- Implement CommitService.GetTreeEntries
  https://gitlab.com/gitlab-org/gitaly/merge_requests/208

## v0.18.0

- Add config to specify a git binary path
  https://gitlab.com/gitlab-org/gitaly/merge_requests/177
- CommitService.CommitsBetween fixes: Invert commits order, populates commit
  message bodies, reject suspicious revisions
  https://gitlab.com/gitlab-org/gitaly/merge_requests/204

## v0.17.0

- Rename auth 'unenforced' to 'transitioning'
  https://gitlab.com/gitlab-org/gitaly/merge_requests/209
- Also check for "refs" folder for repo existence
  https://gitlab.com/gitlab-org/gitaly/merge_requests/207

## v0.16.0

- Implement BlobService.GetBlob
  https://gitlab.com/gitlab-org/gitaly/merge_requests/202

## v0.15.0

- Ensure that sub-processes inherit TZ environment variable
  https://gitlab.com/gitlab-org/gitaly/merge_requests/201
- Implement CommitService::CommitsBetween
  https://gitlab.com/gitlab-org/gitaly/merge_requests/197
- Implement CountCommits RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/203

## v0.14.0

- Added integration test for SSH, and a client package
  https://gitlab.com/gitlab-org/gitaly/merge_requests/178/
- Override gRPC code to Canceled/DeadlineExceeded on requests with
  canceled contexts
  https://gitlab.com/gitlab-org/gitaly/merge_requests/199
- Add RepositoryExists Implementation
  https://gitlab.com/gitlab-org/gitaly/merge_requests/200

## v0.13.0

- Added usage and version flags to the command line interface
  https://gitlab.com/gitlab-org/gitaly/merge_requests/193
- Optional token authentication
  https://gitlab.com/gitlab-org/gitaly/merge_requests/191

## v0.12.0

- Stop using deprecated field `path` in Repository messages
  https://gitlab.com/gitlab-org/gitaly/merge_requests/179
- Implement TreeEntry RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/187

## v0.11.2

Skipping 0.11.1 intentionally, we messed up the tag.

- Add context to structured logging messages
  https://gitlab.com/gitlab-org/gitaly/merge_requests/184
- Fix incorrect dependency in Makefile
  https://gitlab.com/gitlab-org/gitaly/merge_requests/189

## v0.11.0

- FindDefaultBranchName: decorate error
  https://gitlab.com/gitlab-org/gitaly/merge_requests/148
- Hide chatty logs behind GITALY_DEBUG=1. Log access times.
  https://gitlab.com/gitlab-org/gitaly/merge_requests/149
- Count accepted gRPC connections
  https://gitlab.com/gitlab-org/gitaly/merge_requests/151
- Disallow directory traversal in repository paths for security
  https://gitlab.com/gitlab-org/gitaly/merge_requests/152
- FindDefaultBranchName: Handle repos with non-existing HEAD
  https://gitlab.com/gitlab-org/gitaly/merge_requests/164
- Add support for structured logging via logrus
  https://gitlab.com/gitlab-org/gitaly/merge_requests/163
- Add support for exposing the Gitaly build information via Prometheus
  https://gitlab.com/gitlab-org/gitaly/merge_requests/168
- Set GL_PROTOCOL during SmartHTTP.PostReceivePack
  https://gitlab.com/gitlab-org/gitaly/merge_requests/169
- Handle server side errors from shallow clone
  https://gitlab.com/gitlab-org/gitaly/merge_requests/173
- Ensure that grpc server log messages are sent to logrus
  https://gitlab.com/gitlab-org/gitaly/merge_requests/174
- Add support for GRPC Latency Histograms in Prometheus
  https://gitlab.com/gitlab-org/gitaly/merge_requests/172
- Add support for Sentry exception reporting
  https://gitlab.com/gitlab-org/gitaly/merge_requests/171
- CommitDiff: Send chunks of patches over messages
  https://gitlab.com/gitlab-org/gitaly/merge_requests/170
- Upgrade gRPC and its dependencies
  https://gitlab.com/gitlab-org/gitaly/merge_requests/180

## v0.10.0

- CommitDiff: Parse a typechange diff correctly
  https://gitlab.com/gitlab-org/gitaly/merge_requests/136
- CommitDiff: Implement CommitDelta RPC
  https://gitlab.com/gitlab-org/gitaly/merge_requests/139
- PostReceivePack: Set GL_REPOSITORY env variable when provided in request
  https://gitlab.com/gitlab-org/gitaly/merge_requests/137
- Add SSHUpload/ReceivePack Implementation
  https://gitlab.com/gitlab-org/gitaly/merge_requests/132

## v0.9.0

- Add support ignoring whitespace diffs in CommitDiff
  https://gitlab.com/gitlab-org/gitaly/merge_requests/126
- Add support for path filtering in CommitDiff
  https://gitlab.com/gitlab-org/gitaly/merge_requests/126

## v0.8.0

- Don't error on invalid ref in CommitIsAncestor
  https://gitlab.com/gitlab-org/gitaly/merge_requests/129
- Don't error on invalid commit in FindRefName
  https://gitlab.com/gitlab-org/gitaly/merge_requests/122
- Return 'Not Found' gRPC code when repository is not found
  https://gitlab.com/gitlab-org/gitaly/merge_requests/120

## v0.7.0

- Use storage configuration data from config.toml, if possible, when
  resolving repository paths.
  https://gitlab.com/gitlab-org/gitaly/merge_requests/119
- Add CHANGELOG.md
