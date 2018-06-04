module Gitlab
  module Linguist
    class Cache
      OLD_STATS_KEY = 'old_stats'.freeze
      OLD_COMMIT_OID_KEY = 'old_commit_oid'.freeze

      def initialize(repo_path)
        @path = repo_path
      end

      def write(linguist, commit_oid)
        return if old_commit_oid == commit_oid

        FileUtils.mkdir_p(linguist_cache_directory) unless Dir.exist?(linguist_cache_directory)

        new_cache = { OLD_STATS_KEY => linguist.cache, OLD_COMMIT_OID_KEY => commit_oid }

        File.write(cache_path, Marshal.dump(new_cache))
      end

      def old_stats
        cache[OLD_STATS_KEY]
      end

      def old_commit_oid
        cache[OLD_COMMIT_OID_KEY]
      end

      private

      def cache
        @cache ||= if File.exist?(cache_path)
                     Marshal.load(File.binread(cache_path))
                   else
                     {}
                   end
      rescue ArgumentError
        @cache = {}
      end

      def cache_path
        File.join(linguist_cache_directory, 'linguist-cache')
      end

      def linguist_cache_directory
        File.join(@path, 'gitaly')
      end
    end
  end
end
