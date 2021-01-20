module Gitlab
  module Git
    class OperationService
      include Gitlab::Git::Popen

      BranchUpdate = Struct.new(:newrev, :repo_created, :branch_created) do
        alias_method :repo_created?, :repo_created
        alias_method :branch_created?, :branch_created

        def self.from_gitaly(branch_update)
          return if branch_update.nil?

          new(
            branch_update.commit_id,
            branch_update.repo_created,
            branch_update.branch_created
          )
        end
      end

      attr_reader :user, :repository

      def initialize(user, new_repository)
        @user = user
        @repository = new_repository
      end

      def add_branch(branch_name, newrev, transaction: nil)
        ref = Gitlab::Git::BRANCH_REF_PREFIX + branch_name
        oldrev = Gitlab::Git::BLANK_SHA

        update_ref_in_hooks(ref, newrev, oldrev, transaction: transaction)
      end

      def rm_branch(branch, transaction: nil)
        ref = Gitlab::Git::BRANCH_REF_PREFIX + branch.name
        oldrev = branch.target
        newrev = Gitlab::Git::BLANK_SHA

        update_ref_in_hooks(ref, newrev, oldrev, transaction: transaction)
      end

      def add_lightweight_tag(tag_name, tag_target, transaction: nil)
        ref = Gitlab::Git::TAG_REF_PREFIX + tag_name
        oldrev = Gitlab::Git::BLANK_SHA

        update_ref_in_hooks(ref, tag_target, oldrev, transaction: transaction)
      end

      def add_annotated_tag(tag_name, tag_target, transaction, options)
        ref = Gitlab::Git::TAG_REF_PREFIX + tag_name
        oldrev = Gitlab::Git::BLANK_SHA
        annotation = repository.rugged.tags.create_annotation(tag_name, tag_target, options)

        update_ref_in_hooks(ref, annotation.oid, oldrev, transaction: transaction)
      end

      def rm_tag(tag, transaction: nil)
        ref = Gitlab::Git::TAG_REF_PREFIX + tag.name
        oldrev = tag.target
        newrev = Gitlab::Git::BLANK_SHA

        update_ref_in_hooks(ref, newrev, oldrev, transaction: transaction) do
          repository.rugged.tags.delete(tag_name)
        end
      end

      # Whenever `start_branch_name` or `start_sha` is passed, if `branch_name`
      # doesn't exist, it will be created from the commit pointed to by
      # `start_branch_name` or `start_sha`.
      #
      # If `start_repository` is passed, and the branch doesn't exist,
      # it would try to find the commits from it instead of current repository.
      def with_branch(branch_name,
                      start_branch_name: nil,
                      start_sha: nil,
                      start_repository: repository,
                      force: false,
                      &block)
        start_repository = RemoteRepository.new(start_repository) unless start_repository.is_a?(RemoteRepository)

        start_branch_name = nil if start_repository.empty?

        if start_branch_name.present? && !start_repository.branch_exists?(start_branch_name)
          raise ArgumentError, "Cannot find branch '#{start_branch_name}'"
        elsif start_sha.present? && !start_repository.commit_id(start_sha)
          raise ArgumentError, "Cannot find commit '#{start_sha}'"
        end

        update_branch_with_hooks(branch_name, force) do
          repository.with_repo_branch_commit(
            start_repository,
            start_sha.presence || start_branch_name.presence || branch_name,
            &block
          )
        end
      end

      def update_branch(branch_name, newrev, oldrev, push_options: nil, transaction: nil)
        ref = Gitlab::Git::BRANCH_REF_PREFIX + branch_name
        update_ref_in_hooks(ref, newrev, oldrev, push_options: push_options, transaction: transaction)
      end

      # Yields the given block (which should return a commit) and
      # writes it to the ref while also executing hooks for it.
      # The ref is _always_ overwritten (nothing is taken from its
      # previous state).
      #
      # Returns the generated commit.
      #
      # ref - The target ref path we're committing to.
      # from_ref - The ref we're taking the HEAD commit from.
      def commit_ref(ref, source_sha, from_ref:)
        update_autocrlf_option

        target_sha = from_ref.target
        repository.write_ref(ref, target_sha)

        # Make commit
        newrev = yield

        unless newrev
          error = "Failed to create merge commit for source_sha #{source_sha} and" \
                  " target_sha #{target_sha} at #{ref}"

          raise Gitlab::Git::CommitError.new(error)
        end

        oldrev = from_ref.target

        update_ref(ref, newrev, oldrev)

        newrev
      end

      private

      # Returns [newrev, should_run_after_create, should_run_after_create_branch]
      def update_branch_with_hooks(branch_name, force)
        update_autocrlf_option

        was_empty = repository.empty?

        # Make commit
        newrev = yield

        raise Gitlab::Git::CommitError.new('Failed to create commit') unless newrev

        branch = repository.find_branch(branch_name)
        oldrev = find_oldrev_from_branch(newrev, branch, force)

        ref = Gitlab::Git::BRANCH_REF_PREFIX + branch_name
        update_ref_in_hooks(ref, newrev, oldrev)

        BranchUpdate.new(newrev, was_empty, was_empty || Gitlab::Git.blank_ref?(oldrev))
      end

      def find_oldrev_from_branch(newrev, branch, force)
        return Gitlab::Git::BLANK_SHA unless branch

        oldrev = branch.target

        return oldrev if force

        merge_base = repository.merge_base(newrev, branch.target)
        raise Gitlab::Git::Repository::InvalidRef unless merge_base

        if oldrev == merge_base
          oldrev
        else
          raise Gitlab::Git::CommitError.new('Branch diverged')
        end
      end

      def update_ref_in_hooks(ref, newrev, oldrev, push_options: nil, transaction: nil)
        with_hooks(ref, newrev, oldrev, push_options: push_options, transaction: transaction) do
          update_ref(ref, newrev, oldrev)
        end
      end

      def with_hooks(ref, newrev, oldrev, push_options: nil, transaction: nil)
        Gitlab::Git::HooksService.new.execute(
          user,
          repository,
          oldrev,
          newrev,
          ref,
          push_options: push_options,
          transaction: transaction
        ) do |service|
          yield(service)
        end
      end

      def update_ref(ref, newrev, oldrev)
        # We use 'git update-ref' because libgit2/rugged currently does not
        # offer 'compare and swap' ref updates. Without compare-and-swap we can
        # (and have!) accidentally reset the ref to an earlier state, clobbering
        # commits. See also https://github.com/libgit2/libgit2/issues/1534.
        command = %W[#{Gitlab.config.git.bin_path} update-ref --stdin -z]

        output, status = popen(
          command,
          repository.path
        ) do |stdin|
          stdin.write("update #{ref}\x00#{newrev}\x00#{oldrev}\x00")
        end

        unless status.zero?
          Gitlab::GitLogger.error("'git update-ref' in #{repository.path}: #{output}")
          ref_name = Gitlab::Git.branch_name(ref) || ref

          raise Gitlab::Git::CommitError.new(
            "Could not update #{ref_name}." \
            " Please refresh and try again."
          )
        end
      end

      def update_autocrlf_option
        repository.autocrlf = :input if repository.autocrlf != :input
      end
    end
  end
end
