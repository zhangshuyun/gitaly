module Gitlab
  module Linguist
    class RepositoryLanguages
      def initialize(repo, commit)
        @repo = repo
        @commit = commit
        @cache = Gitlab::Linguist::Cache.new(repo.path)
      end

      def detect
        linguist = ::Linguist::Repository.incremental(@repo.rugged, @commit.id, @cache.old_commit_oid, @cache.old_stats)

        languages = linguist
          .languages
          .sort_by { |_k, v| v }
          .reverse

        @cache.write(linguist, @commit.id)

        languages
      end
    end
  end
end
