module GitalyServer
  class CommitService < Gitaly::CommitService::Service
    def commit_languages(request, _call)
      repo = Gitlab::Git::Repository.from_call(_call)
      revision = request.revision unless request.revision.empty?

      language_messages = repo.languages(revision).map do |language|
        Gitaly::CommitLanguagesResponse::Language.new(
          name: language[:label],
          share: language[:value],
          color: language[:color]
        )
      end

      Gitaly::CommitLanguagesResponse.new(languages: language_messages)
    end

    def find_commits(request, _call)
      repository = Gitlab::Git::Repository.from_call(_call)
      options = {
        ref: request.revision,
        limit: request.limit,
        follow: request.follow,
        skip_merges: request.skip_merges,
        disable_walk: request.disable_walk,
        offset: request.offset,
      }
      options[:path] = request.paths unless request.paths.empty?

      options[:before] = Time.at(request.before.seconds).to_datetime if request.before
      options[:after] = Time.at(request.after.seconds).to_datetime if request.after

      Enumerator.new do |y|
        # Send back 'pages' with 20 commits each
        repository.raw_log(options).each_slice(20) do |rugged_commits|
          commits = rugged_commits.map do |rugged_commit|
            gitaly_commit_from_rugged(rugged_commit)
          end
          y.yield Gitaly::FindCommitsResponse.new(commits: commits)
        end
      end
    end

    def gitaly_commit_from_rugged(rugged_commit)
      Gitaly::GitCommit.new(
        id: rugged_commit.oid,
        subject: rugged_commit.message.split("\n", 2)[0].chomp,
        body: rugged_commit.message,
        parent_ids: rugged_commit.parent_ids,
        author: gitaly_commit_author_from_rugged(rugged_commit.author),
        committer: gitaly_commit_author_from_rugged(rugged_commit.committer),
      )
    end

    def gitaly_commit_author_from_rugged(rugged_author)
      Gitaly::CommitAuthor.new(
        name: bytes!(rugged_author[:name]),
        email: bytes!(rugged_author[:email]),
        date: Google::Protobuf::Timestamp.new(seconds: rugged_author[:time].to_i)
      )
    end

    def bytes!(string)
      string.force_encoding('ASCII-8BIT')
    end
  end
end
