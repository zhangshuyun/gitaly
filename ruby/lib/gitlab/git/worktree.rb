# frozen_string_literal: true

module Gitlab
  module Git
    class Worktree
      attr_reader :path, :name

      def initialize(repo_path, prefix, id)
        @repo_path = repo_path
        @prefix = prefix
        @id = id.to_s
        @name = "#{prefix}-#{id}"
        @path = worktree_path
      end

      private

      def worktree_path
        raise ArgumentError, "worktree id can't be empty" unless @id.present?
        raise ArgumentError, "worktree id can't contain slashes " if @id.include?("/")

        File.join(@repo_path, 'gitlab-worktree', @name)
      end
    end
  end
end
