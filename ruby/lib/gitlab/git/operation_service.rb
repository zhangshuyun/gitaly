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

        # Refactoring aid
        Gitlab::Git.check_namespace!(new_repository)

        @repository = new_repository
      end

      def add_branch(branch_name, newrev)
        ref = Gitlab::Git::BRANCH_REF_PREFIX + branch_name
        oldrev = Gitlab::Git::BLANK_SHA

        update_ref_in_hooks(ref, newrev, oldrev)
      end

      def rm_branch(branch)
        ref = Gitlab::Git::BRANCH_REF_PREFIX + branch.name
        oldrev = branch.target
        newrev = Gitlab::Git::BLANK_SHA

        update_ref_in_hooks(ref, newrev, oldrev)
      end

      def add_tag(tag_name, newrev, options = {})
        ref = Gitlab::Git::TAG_REF_PREFIX + tag_name
        oldrev = Gitlab::Git::BLANK_SHA

        with_hooks(ref, newrev, oldrev) do |service|
          # We want to pass the OID of the tag object to the hooks. For an
          # annotated tag we don't know that OID until after the tag object
          # (raw_tag) is created in the repository. That is why we have to
          # update the value after creating the tag object. Only the
          # "post-receive" hook will receive the correct value in this case.
          raw_tag = repository.rugged.tags.create(tag_name, newrev, options)
          service.newrev = raw_tag.target_id
        end
      end

      def rm_tag(tag)
        ref = Gitlab::Git::TAG_REF_PREFIX + tag.name
        oldrev = tag.target
        newrev = Gitlab::Git::BLANK_SHA

        update_ref_in_hooks(ref, newrev, oldrev) do
          repository.rugged.tags.delete(tag_name)
        end
      end

      # Whenever `start_branch_name` is passed, if `branch_name` doesn't exist,
      # it would be created from `start_branch_name`.
      # If `start_repository` is passed, and the branch doesn't exist,
      # it would try to find the commits from it instead of current repository.
      def with_branch(
        branch_name,
        start_branch_name: nil,
        start_repository: repository,
        force: false,
        &block
      )

        Gitlab::Git.check_namespace!(start_repository)
        start_repository = RemoteRepository.new(start_repository) unless start_repository.is_a?(RemoteRepository)

        start_branch_name = nil if start_repository.empty?

        raise ArgumentError, "Cannot find branch #{start_branch_name} in #{start_repository.relative_path}" if start_branch_name && !start_repository.branch_exists?(start_branch_name)

        update_branch_with_hooks(branch_name, force) do
          repository.with_repo_branch_commit(
            start_repository,
            start_branch_name || branch_name,
            &block
          )
        end
      end

      def update_branch(branch_name, newrev, oldrev)
        ref = Gitlab::Git::BRANCH_REF_PREFIX + branch_name
        update_ref_in_hooks(ref, newrev, oldrev)
      end

      # Yields the given block (which should return a commit) and
      # writes it to the ref while also executing hooks for it.
      # The ref is _always_ overwritten (nothing is taken from its
      # previous state).
      #
      # Returns the generated commit.
      #
      # ref - The target ref path we're commiting to.
      # from_branch - The branch we're taking the HEAD commit from.
      def commit_ref(ref, from_branch:)
        update_autocrlf_option

        repository.write_ref(ref, from_branch.target)

        # Make commit
        newrev = yield

        raise Gitlab::Git::CommitError.new('Failed to create commit') unless newrev

        oldrev = from_branch.target

        update_ref_in_hooks(ref, newrev, oldrev)

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

      def update_ref_in_hooks(ref, newrev, oldrev)
        with_hooks(ref, newrev, oldrev) do
          update_ref(ref, newrev, oldrev)
        end
      end

      def with_hooks(ref, newrev, oldrev)
        Gitlab::Git::HooksService.new.execute(
          user,
          repository,
          oldrev,
          newrev,
          ref
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
          raise Gitlab::Git::CommitError.new(
            "Could not update branch #{Gitlab::Git.branch_name(ref)}." \
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
