module Gitlab
  module Git
    class Commit
      include Gitlab::EncodingHelper

      attr_accessor :raw_commit, :head

      MAX_COMMIT_MESSAGE_DISPLAY_SIZE = 10.megabytes
      MIN_SHA_LENGTH = 7
      SERIALIZE_KEYS = %i[
        id message parent_ids
        authored_date author_name author_email
        committed_date committer_name committer_email trailers
      ].freeze

      attr_accessor *SERIALIZE_KEYS # rubocop:disable Lint/AmbiguousOperator

      def ==(other)
        return false unless other.is_a?(Gitlab::Git::Commit)

        id && id == other.id
      end

      class << self
        # Get single commit
        #
        # Ex.
        #   Commit.find(repo, '29eda46b')
        #
        #   Commit.find(repo, 'master')
        #
        def find(repo, commit_id = "HEAD")
          # Already a commit?
          return commit_id if commit_id.is_a?(Gitlab::Git::Commit)

          # A rugged reference?
          commit_id = Gitlab::Git::Ref.dereference_object(commit_id)
          return decorate(repo, commit_id) if commit_id.is_a?(Rugged::Commit)

          # Some weird thing?
          return nil unless commit_id.is_a?(String)

          # This saves us an RPC round trip.
          return nil if commit_id.include?(':')

          commit = rugged_find(repo, commit_id)

          decorate(repo, commit) if commit
        rescue Rugged::ReferenceError, Rugged::InvalidError, Rugged::ObjectError,
               Gitlab::Git::CommandError, Gitlab::Git::Repository::NoRepository,
               Rugged::OdbError, Rugged::TreeError, ArgumentError
          nil
        end

        def rugged_find(repo, commit_id)
          obj = repo.rev_parse_target(commit_id)

          obj.is_a?(Rugged::Commit) ? obj : nil
        end

        def decorate(repository, commit, ref = nil)
          Gitlab::Git::Commit.new(repository, commit, ref)
        end

        def shas_with_signatures(repository, shas)
          shas.select do |sha|
            begin
              Rugged::Commit.extract_signature(repository.rugged, sha)
            rescue Rugged::OdbError
              false
            end
          end
        end
      end

      def initialize(repository, raw_commit, head = nil)
        raise "Nil as raw commit passed" unless raw_commit

        @repository = repository
        @head = head

        case raw_commit
        when Hash
          init_from_hash(raw_commit)
        when Rugged::Commit
          init_from_rugged(raw_commit)
        when Gitaly::GitCommit
          init_from_gitaly(raw_commit)
        else
          raise "Invalid raw commit type: #{raw_commit.class}"
        end
      end

      def sha
        id
      end

      def short_id(length = 10)
        id.to_s[0..length]
      end

      def safe_message
        @safe_message ||= message
      end

      def no_commit_message
        "--no commit message"
      end

      def to_hash
        serialize_keys.map.with_object({}) do |key, hash|
          hash[key] = send(key)
        end
      end

      def date
        committed_date
      end

      def parents
        parent_ids.map { |oid| self.class.find(@repository, oid) }.compact
      end

      def message
        encode! @message
      end

      def author_name
        encode! @author_name
      end

      def author_email
        encode! @author_email
      end

      def committer_name
        encode! @committer_name
      end

      def committer_email
        encode! @committer_email
      end

      def rugged_commit
        @rugged_commit ||= if raw_commit.is_a?(Rugged::Commit)
                             raw_commit
                           else
                             @repository.rev_parse_target(id)
                           end
      end

      def merge_commit?
        parent_ids.size > 1
      end

      def to_gitaly_commit
        return raw_commit if raw_commit.is_a?(Gitaly::GitCommit)

        message_split = raw_commit.message.split("\n", 2)
        Gitaly::GitCommit.new(
          id: raw_commit.oid,
          subject: message_split[0] ? message_split[0].chomp.b : "",
          body: raw_commit.message.b,
          parent_ids: raw_commit.parent_ids,
          author: gitaly_commit_author_from_rugged(raw_commit.author),
          committer: gitaly_commit_author_from_rugged(raw_commit.committer),
          trailers: gitaly_trailers_from_rugged(raw_commit)
        )
      end

      private

      def init_from_hash(hash)
        raw_commit = hash.symbolize_keys

        serialize_keys.each do |key|
          send("#{key}=", raw_commit[key])
        end
      end

      def init_from_rugged(commit)
        author = commit.author
        committer = commit.committer

        @raw_commit = commit
        @id = commit.oid
        @message = commit.message
        @authored_date = author[:time]
        @committed_date = committer[:time]
        @author_name = author[:name]
        @author_email = author[:email]
        @committer_name = committer[:name]
        @committer_email = committer[:email]
        @parent_ids = commit.parents.map(&:oid)
        @trailers = Hash[commit.trailers]
      end

      def init_from_gitaly(commit)
        @raw_commit = commit
        @id = commit.id
        # TODO: Once gitaly "takes over" Rugged consider separating the
        # subject from the message to make it clearer when there's one
        # available but not the other.
        @message = message_from_gitaly_body
        @authored_date = Time.at(commit.author.date.seconds).utc
        @author_name = commit.author.name.dup
        @author_email = commit.author.email.dup
        @committed_date = Time.at(commit.committer.date.seconds).utc
        @committer_name = commit.committer.name.dup
        @committer_email = commit.committer.email.dup
        @parent_ids = Array(commit.parent_ids)
        @trailers = Hash[commit.trailers.map { |t| [t.key, t.value] }]
      end

      def serialize_keys
        SERIALIZE_KEYS
      end

      def gitaly_commit_author_from_rugged(author_or_committer)
        Gitaly::CommitAuthor.new(
          name: author_or_committer[:name].b,
          email: author_or_committer[:email].b,
          date: Google::Protobuf::Timestamp.new(seconds: author_or_committer[:time].to_i)
        )
      end

      def gitaly_trailers_from_rugged(rugged_commit)
        rugged_commit.trailers.map do |(key, value)|
          Gitaly::CommitTrailer.new(key: key, value: value)
        end
      end

      def message_from_gitaly_body
        return @raw_commit.subject.dup if @raw_commit.body_size.zero?

        @raw_commit.body.dup
      end
    end
  end
end
