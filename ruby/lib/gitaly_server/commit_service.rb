module GitalyServer
  class CommitService < Gitaly::CommitService::Service
    include Utils
    include Gitlab::EncodingHelper

    def commit_stats(request, call)
      repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
      revision = request.revision unless request.revision.empty?

      commit = Gitlab::Git::Commit.find(repo, revision)

      # In the odd case that the revision given doesn't exist we need to raise
      # an exception. Since GitLab (currently) already does this for us we don't
      # expect this to actually happen, just guarding against future code change
      raise GRPC::Internal.new("commit not found for revision '#{revision}'") unless commit

      stats = Gitlab::Git::CommitStats.new(repo, commit)

      Gitaly::CommitStatsResponse.new(oid: stats.id, additions: stats.additions, deletions: stats.deletions)
    end

    def find_commits(request, call)
      repository = Gitlab::Git::Repository.from_gitaly(request.repository, call)
      options = {
        ref: request.revision,
        limit: request.limit,
        follow: request.follow,
        skip_merges: request.skip_merges,
        disable_walk: request.disable_walk,
        offset: request.offset,
        all: request.all
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

    def filter_shas_with_signatures(_session, call)
      Enumerator.new do |y|
        repository = nil

        call.each_remote_read.with_index do |request, index|
          repository = Gitlab::Git::Repository.from_gitaly(request.repository, call) if index.zero?

          y << Gitaly::FilterShasWithSignaturesResponse.new(shas: Gitlab::Git::Commit.shas_with_signatures(repository, request.shas))
        end
      end
    end

    def extract_commit_signature(request, call)
      repository = Gitlab::Git::Repository.from_gitaly(request.repository, call)

      Enumerator.new do |y|
        each_commit_signature_chunk(repository, request.commit_id) do |signature_chunk, signed_text_chunk|
          y.yield Gitaly::ExtractCommitSignatureResponse.new(signature: signature_chunk) if signature_chunk.present?

          y.yield Gitaly::ExtractCommitSignatureResponse.new(signed_text: signed_text_chunk) if signed_text_chunk.present?
        end
      end
    end

    def get_commit_signatures(request, call)
      repository = Gitlab::Git::Repository.from_gitaly(request.repository, call)

      Enumerator.new do |y|
        request.commit_ids.each do |commit_id|
          msg = Gitaly::GetCommitSignaturesResponse.new(commit_id: commit_id)

          each_commit_signature_chunk(repository, commit_id) do |signature_chunk, signed_text_chunk|
            if signature_chunk.present?
              msg.signature = signature_chunk
              y.yield msg
            end

            if signed_text_chunk.present?
              msg.signed_text = signed_text_chunk
              y.yield msg
            end

            msg = Gitaly::GetCommitSignaturesResponse.new
          end
        end
      end
    end

    def get_commit_messages(request, call)
      repository = Gitlab::Git::Repository.from_gitaly(request.repository, call)

      Enumerator.new do |y|
        request.commit_ids.each do |commit_id|
          commit = Gitlab::Git::Commit.find(repository, commit_id)
          next unless commit

          response = Gitaly::GetCommitMessagesResponse.new(commit_id: commit.id)
          io = StringIO.new(commit.message)

          while chunk = io.read(Gitlab.config.git.max_commit_or_tag_message_size)
            response.message = chunk

            y.yield response

            response = Gitaly::GetCommitMessagesResponse.new
          end
        end
      end
    end

    private

    # yields either signature chunks or signed_text chunks to the passed block
    def each_commit_signature_chunk(repository, commit_id)
      raise ArgumentError.new("expected a block") unless block_given?

      begin
        signature_text, signed_text = Rugged::Commit.extract_signature(repository.rugged, commit_id)
      rescue Rugged::InvalidError
        raise GRPC::InvalidArgument.new("commit lookup failed for #{commit_id.inspect}")
      rescue Rugged::OdbError
        # The client does not care if the commit does not exist
        return
      end

      signature_text_io = binary_stringio(signature_text)
      loop do
        chunk = signature_text_io.read(Gitlab.config.git.write_buffer_size)
        break if chunk.nil?

        yield chunk, nil
      end

      signed_text_io = binary_stringio(signed_text)
      loop do
        chunk = signed_text_io.read(Gitlab.config.git.write_buffer_size)
        break if chunk.nil?

        yield nil, chunk
      end
    end
  end
end
