module Gitlab
  module Git
    class LfsChanges
      def initialize(repository, newrev)
        @repository = repository
        @newrev = newrev
      end

      def new_pointers(object_limit: nil, not_in: nil)
        git_new_pointers(object_limit, not_in)
      end

      def all_pointers
        git_all_pointers
      end

      private

      def git_new_pointers(object_limit, not_in)
        params = {
          options: ["--filter=blob:limit=#{Gitlab::Git::Blob::LFS_POINTER_MAX_SIZE}"],
          not_in: not_in
        }

        rev_list.new_objects(**rev_list_params(params)) do |object_ids|
          Gitlab::Git::Blob.batch_lfs_pointers(@repository, object_ids, limit: object_limit)
        end
      end

      def git_all_pointers
        params = { options: ["--filter=blob:limit=#{Gitlab::Git::Blob::LFS_POINTER_MAX_SIZE}"] }

        rev_list.all_objects(**rev_list_params(params)) do |object_ids|
          Gitlab::Git::Blob.batch_lfs_pointers(@repository, object_ids)
        end
      end

      def rev_list
        Gitlab::Git::RevList.new(@repository, newrev: @newrev)
      end

      # We're passing the `--in-commit-order` arg to ensure we don't wait
      # for git to traverse all commits before returning pointers.
      # This is required in order to improve the performance of LFS integrity check
      def rev_list_params(params = {})
        params[:options] ||= []
        params[:options] << "--in-commit-order"
        params[:require_path] = true

        params
      end
    end
  end
end
