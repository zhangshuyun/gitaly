require 'linguist'

module GitalyServer
  class CommitService < Gitaly::CommitService::Service
    include Utils
    include Gitlab::EncodingHelper

    def commit_stats(request, call)
      bridge_exceptions do
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
    end

    # TODO remove this implementation in GitLab 11.3
    def list_commits_by_oid(request, call)
      bridge_exceptions do
        repository = Gitlab::Git::Repository.from_gitaly(request.repository, call)

        Enumerator.new do |y|
          request.oid.each_slice(20) do |oids|
            commits = oids.map do |oid|
              commit =
                begin
                  repository.rev_parse_target(oid)
                rescue Rugged::ReferenceError, Rugged::InvalidError
                  nil
                end

              commit.is_a?(Rugged::Commit) ? gitaly_commit_from_rugged(commit) : nil
            end.compact

            y.yield Gitaly::ListCommitsByOidResponse.new(commits: commits)
          end
        end
      end
    end

    def find_commits(request, call)
      bridge_exceptions do
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
    end

    def filter_shas_with_signatures(session, call)
      Enumerator.new do |y|
        bridge_exceptions do
          repository = nil

          call.each_remote_read.with_index do |request, index|
            if index.zero?
              repository = Gitlab::Git::Repository.from_gitaly(request.repository, call)
            end

            y << Gitaly::FilterShasWithSignaturesResponse.new(shas: Gitlab::Git::Commit.shas_with_signatures(repository, request.shas))
          end
        end
      end
    end

    def extract_commit_signature(request, call)
      bridge_exceptions do
        repository = Gitlab::Git::Repository.from_gitaly(request.repository, call)

        Enumerator.new do |y|
          each_commit_signature_chunk(repository, request.commit_id) do |signature_chunk, signed_text_chunk|
            if signature_chunk.present?
              y.yield Gitaly::ExtractCommitSignatureResponse.new(signature: signature_chunk)
            end

            if signed_text_chunk.present?
              y.yield Gitaly::ExtractCommitSignatureResponse.new(signed_text: signed_text_chunk)
            end
          end
        end
      end
    end

    def get_commit_signatures(request, call)
      bridge_exceptions do
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
    end

    def get_commit_messages(request, call)
      bridge_exceptions do
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
    end

    def commit_languages(request, call)
      bridge_exceptions do
        repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)

        commit = Gitlab::Git::Commit.find(repo, request.revision)
        raise GRPC::InvalidArgument, 'revision could not be resolved' unless commit

        languages = Linguist::Repository.new(repo.rugged, commit.id)
          .languages
          .sort_by { |_k, v| v }
          .reverse

        total_bytes = languages.sum(&:last)
        return Gitaly::CommitLanguagesResponse.new(languages: []) if total_bytes == 0

        languages.map! do |name, bytes|
          warn "#{bytes} of a total of #{total_bytes}" if name == 'Ruby'

          Gitaly::CommitLanguagesResponse::Language.new(
            name: name.to_s,
            share: ((bytes.to_f / total_bytes.to_f) * 100).round,
            color: linguist_color(name)
          )
        end

        Gitaly::CommitLanguagesResponse.new(languages: languages)
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

    def linguist_color(language)
      Linguist::Language.find_by_name(language)&.color || "##{Digest::SHA256.hexdigest(language)[0..5]}"
    end
  end
end
