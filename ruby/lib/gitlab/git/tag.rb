require_relative 'ref'

module Gitlab
  module Git
    class Tag < Ref
      extend Gitlab::EncodingHelper

      attr_reader :object_sha, :repository

      SERIALIZE_KEYS = %i[name target target_commit message].freeze

      attr_accessor *SERIALIZE_KEYS # rubocop:disable Lint/AmbiguousOperator

      def initialize(repository, raw_tag)
        @repository = repository
        @raw_tag = raw_tag

        case raw_tag
        when Hash
          init_from_hash
        when Gitaly::Tag
          init_from_gitaly
        end

        super(repository, name, target, target_commit)
      end

      def init_from_hash
        raw_tag = @raw_tag.symbolize_keys

        SERIALIZE_KEYS.each do |key|
          send("#{key}=", raw_tag[key]) # rubocop:disable GitlabSecurity/PublicSend
        end
      end

      def init_from_gitaly
        @name = encode!(@raw_tag.name.dup)
        @target = @raw_tag.id
        @message = @raw_tag.message.dup

        if @raw_tag.target_commit.present?
          @target_commit = Gitlab::Git::Commit.decorate(repository, @raw_tag.target_commit)
        end
      end

      def message
        encode! @message
      end
    end
  end
end
