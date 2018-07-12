module Gitlab
  module Git
    # These are monkey patches on top of the vendored version of Repository.
    class Repository
      class << self
        def from_gitaly(gitaly_repository, call)
          new(
            gitaly_repository,
            GitalyServer.repo_path(call),
            GitalyServer.gl_repository(call),
            Gitlab::Git::GitlabProjects.from_gitaly(gitaly_repository, call),
            GitalyServer.repo_alt_dirs(call)
          )
        end

        def create(repo_path)
          FileUtils.mkdir_p(repo_path, mode: 0770)

          # Equivalent to `git --git-path=#{repo_path} init [--bare]`
          repo = Rugged::Repository.init_at(repo_path, true)
          repo.close

          symlink_hooks_to = Gitlab.config.gitlab_shell.hooks_path
          create_hooks(repo_path, symlink_hooks_to) if symlink_hooks_to.present?
        end

        def create_hooks(repo_path, global_hooks_path)
          local_hooks_path = File.join(repo_path, 'hooks')
          real_local_hooks_path = :not_found

          begin
            real_local_hooks_path = File.realpath(local_hooks_path)
          rescue Errno::ENOENT
            # real_local_hooks_path == :not_found
          end

          # Do nothing if hooks already exist
          unless real_local_hooks_path == File.realpath(global_hooks_path)
            if File.exist?(local_hooks_path)
              # Move the existing hooks somewhere safe
              FileUtils.mv(
                local_hooks_path,
                "#{local_hooks_path}.old.#{Time.now.to_i}")
            end

            # Create the hooks symlink
            FileUtils.ln_sf(global_hooks_path, local_hooks_path)
          end

          true
        end
      end

      attr_reader :path

      def initialize(gitaly_repository, path, gl_repository, gitlab_projects, combined_alt_dirs="")
        @gitaly_repository = gitaly_repository

        @alternate_object_directories = combined_alt_dirs
          .split(File::PATH_SEPARATOR)
          .map { |d| File.join(path, d) }

        @storage = gitaly_repository.storage_name
        @relative_path = gitaly_repository.relative_path
        @path = path
        @gl_repository = gl_repository
        @gitlab_projects = gitlab_projects
      end

      def add_branch(branch_name, user:, target:)
        target_object = Ref.dereference_object(lookup(target))
        raise InvalidRef.new("target not found: #{target}") unless target_object

        OperationService.new(user, self).add_branch(branch_name, target_object.oid)
        find_branch(branch_name)
      rescue Rugged::ReferenceError => ex
        raise InvalidRef, ex
      end

      # Fake implementation, so we wrap correctly on the client side
      def wrapped_gitaly_errors
        yield
      end

      def circuit_breaker
        FakeCircuitBreaker
      end

      def gitaly_repository
        @gitaly_repository
      end

      def alternate_object_directories
        @alternate_object_directories
      end

      def relative_object_directories
        raise "don't use relative object directories in gitaly-ruby"
      end

      # TODO: Can be removed once https://gitlab.com/gitlab-org/gitaly/merge_requests/738
      #       is well and truly out in the wild.
      def fsck
        msg, status = run_git(%W[--git-dir=#{path} fsck], nice: true)
        raise GitError.new("Could not fsck repository: #{msg}") unless status.zero?
      end

      def exists?
        File.exist?(File.join(path, 'refs'))
      end

      def root_ref
        @root_ref ||= discover_default_branch
      end

      def branch_names
        branches.map(&:name)
      end

      def branches
        branches_filter
      end

      def local_branches(sort_by: nil)
        branches_filter(filter: :local, sort_by: sort_by)
      end

      def has_local_branches_rugged?
        branches_filter(filter: :local).any? do |ref|
          begin
            ref.name && ref.target # ensures the branch is valid

            true
          rescue Rugged::ReferenceError
            false
          end
        end
      end

      def tag_names
        rugged.tags.map { |t| t.name }
      end

      def tags
        rugged.references.each("refs/tags/*").map do |ref|
          message = nil

          if ref.target.is_a?(Rugged::Tag::Annotation)
            tag_message = ref.target.message

            if tag_message.respond_to?(:chomp)
              message = tag_message.chomp
            end
          end

          target_commit = Gitlab::Git::Commit.find(self, ref.target)
          Gitlab::Git::Tag.new(self, {
            name: ref.name,
            target: ref.target,
            target_commit: target_commit,
            message: message
          })
        end.sort_by(&:name)
      end

      # Discovers the default branch based on the repository's available branches
      #
      # - If no branches are present, returns nil
      # - If one branch is present, returns its name
      # - If two or more branches are present, returns current HEAD or master or first branch
      def discover_default_branch
        names = branch_names

        return if names.empty?

        return names[0] if names.length == 1

        if rugged_head
          extracted_name = Ref.extract_branch_name(rugged_head.name)

          return extracted_name if names.include?(extracted_name)
        end

        if names.include?('master')
          'master'
        else
          names[0]
        end
      end

      def write_config(full_path:)
        return unless full_path.present?

        raise NoRepository, 'repository does not exist' unless exists?

        rugged.config['gitlab.fullpath'] = full_path
      end

      def ancestor?(from, to)
        return false if from.nil? || to.nil?

        rugged_merge_base(from, to) == from
      rescue Rugged::OdbError
        false
      end

      # old_rev and new_rev are commit ID's
      # the result of this method is an array of Gitlab::Git::RawDiffChange
      def raw_changes_between(old_rev, new_rev)
        @raw_changes_between ||= {}

        @raw_changes_between[[old_rev, new_rev]] ||=
          begin
            return [] if new_rev.blank? || new_rev == Gitlab::Git::BLANK_SHA
            return [] unless new_changes_in_rev?(new_rev)

            result = []

            circuit_breaker.perform do
              Open3.pipeline_r(git_diff_cmd(old_rev, new_rev), format_git_cat_file_script, git_cat_file_cmd) do |last_stdout, wait_threads|
                last_stdout.each_line { |line| result << ::Gitlab::Git::RawDiffChange.new(line.chomp!) }

                if wait_threads.any? { |waiter| !waiter.value&.success? }
                  raise ::Gitlab::Git::Repository::GitError, "Unabled to obtain changes between #{old_rev} and #{new_rev}"
                end
              end
            end

            result
          end
      rescue ArgumentError => e
        raise Gitlab::Git::Repository::GitError.new(e)
      end

      def new_changes_in_rev?(rev)
        revs = run_git!(%W[rev-list #{rev} --not --all --objects --max-count=1]).strip

        File.open('/Users/ruben/projects/gdk-ee/gitlab/log.txt', 'w') { |f| f.puts "OUT: #{revs.class}. #{revs.inspect}" }

        revs.present?
      end

      def add_tag(tag_name, user:, target:, message: nil)
        target_object = Ref.dereference_object(lookup(target))
        raise InvalidRef.new("target not found: #{target}") unless target_object

        user = Gitlab::Git::User.from_gitlab(user) unless user.respond_to?(:gl_id)

        options = nil # Use nil, not the empty hash. Rugged cares about this.
        if message
          options = {
            message: message,
            tagger: Gitlab::Git.committer_hash(email: user.email, name: user.name)
          }
        end

        Gitlab::Git::OperationService.new(user, self).add_tag(tag_name, target_object.oid, options)

        find_tag(tag_name)
      rescue Rugged::ReferenceError => ex
        raise InvalidRef, ex
      rescue Rugged::TagError
        raise TagExistsError
      end

      def rm_branch(branch_name, user:)
        OperationService.new(user, self).rm_branch(find_branch(branch_name))
      end

      def rm_tag(tag_name, user:)
        Gitlab::Git::OperationService.new(user, self).rm_tag(find_tag(tag_name))
      end

      def merge(user, source_sha, target_branch, message, &block)
        committer = Gitlab::Git.committer_hash(email: user.email, name: user.name)

        OperationService.new(user, self).with_branch(target_branch) do |start_commit|
          our_commit = start_commit.sha
          their_commit = source_sha

          raise 'Invalid merge target' unless our_commit
          raise 'Invalid merge source' unless their_commit

          merge_index = rugged.merge_commits(our_commit, their_commit)
          break if merge_index.conflicts?

          options = {
            parents: [our_commit, their_commit],
            tree: merge_index.write_tree(rugged),
            message: message,
            author: committer,
            committer: committer
          }

          commit_id = create_commit(options)

          yield commit_id

          commit_id
        end
      rescue Gitlab::Git::CommitError # when merge_index.conflicts?
        nil
      end

      def ff_merge(user, source_sha, target_branch)
        OperationService.new(user, self).with_branch(target_branch) do |our_commit|
          raise ArgumentError, 'Invalid merge target' unless our_commit

          source_sha
        end
      rescue Rugged::ReferenceError, InvalidRef
        raise ArgumentError, 'Invalid merge source'
      end

      def revert(user:, commit:, branch_name:, message:, start_branch_name:, start_repository:)
        OperationService.new(user, self).with_branch(
          branch_name,
          start_branch_name: start_branch_name,
          start_repository: start_repository
        ) do |start_commit|

          Gitlab::Git.check_namespace!(commit, start_repository)

          revert_tree_id = check_revert_content(commit, start_commit.sha)
          raise CreateTreeError unless revert_tree_id

          committer = user_to_committer(user)

          create_commit(message: message,
                        author: committer,
                        committer: committer,
                        tree: revert_tree_id,
                        parents: [start_commit.sha])
        end
      end

      def cherry_pick(user:, commit:, branch_name:, message:, start_branch_name:, start_repository:)
        args = {
          user: user,
          commit: commit,
          branch_name: branch_name,
          message: message,
          start_branch_name: start_branch_name,
          start_repository: start_repository
        }

        rugged_cherry_pick(args)
      end

      def rebase(user, rebase_id, branch:, branch_sha:, remote_repository:, remote_branch:)
        rebase_path = worktree_path(REBASE_WORKTREE_PREFIX, rebase_id)
        env = git_env_for_user(user)

        if remote_repository.is_a?(RemoteRepository)
          env.merge!(remote_repository.fetch_env)
          remote_repo_path = GITALY_INTERNAL_URL
        else
          remote_repo_path = remote_repository.path
        end

        with_worktree(rebase_path, branch, env: env) do
          run_git!(
            %W(pull --rebase #{remote_repo_path} #{remote_branch}),
            chdir: rebase_path, env: env
          )

          rebase_sha = run_git!(%w(rev-parse HEAD), chdir: rebase_path, env: env).strip

          update_branch(branch, user: user, newrev: rebase_sha, oldrev: branch_sha)

          rebase_sha
        end
      end

      def squash(user, squash_id, branch:, start_sha:, end_sha:, author:, message:)
        squash_path = worktree_path(SQUASH_WORKTREE_PREFIX, squash_id)
        env = git_env_for_user(user).merge(
          'GIT_AUTHOR_NAME' => author.name,
          'GIT_AUTHOR_EMAIL' => author.email
        )
        diff_range = "#{start_sha}...#{end_sha}"
        diff_files = run_git!(
          %W(diff --name-only --diff-filter=ar --binary #{diff_range})
        ).chomp

        with_worktree(squash_path, branch, sparse_checkout_files: diff_files, env: env) do
          # Apply diff of the `diff_range` to the worktree
          diff = run_git!(%W(diff --binary #{diff_range}))
          run_git!(%w(apply --index --whitespace=nowarn), chdir: squash_path, env: env) do |stdin|
            stdin.binmode
            stdin.write(diff)
          end

          # Commit the `diff_range` diff
          run_git!(%W(commit --no-verify --message #{message}), chdir: squash_path, env: env)

          # Return the squash sha. May print a warning for ambiguous refs, but
          # we can ignore that with `--quiet` and just take the SHA, if present.
          # HEAD here always refers to the current HEAD commit, even if there is
          # another ref called HEAD.
          run_git!(
            %w(rev-parse --quiet --verify HEAD), chdir: squash_path, env: env
          ).chomp
        end
      end

      def multi_action(
        user, branch_name:, message:, actions:,
        author_email: nil, author_name: nil,
        start_branch_name: nil, start_repository: self)

        OperationService.new(user, self).with_branch(
          branch_name,
          start_branch_name: start_branch_name,
          start_repository: start_repository
        ) do |start_commit|

          index = Gitlab::Git::Index.new(self)
          parents = []

          if start_commit
            index.read_tree(start_commit.rugged_commit.tree)
            parents = [start_commit.sha]
          end

          actions.each { |opts| index.apply(opts.delete(:action), opts) }

          committer = user_to_committer(user)
          author = Gitlab::Git.committer_hash(email: author_email, name: author_name) || committer
          options = {
            tree: index.write_tree,
            message: message,
            parents: parents,
            author: author,
            committer: committer
          }

          create_commit(options)
        end
      end

      def raw_log(options)
        sha =
          unless options[:all]
            actual_ref = options[:ref] || root_ref
            begin
              sha_from_ref(actual_ref)
            rescue Rugged::OdbError, Rugged::InvalidError, Rugged::ReferenceError
              # Return an empty array if the ref wasn't found
              return []
            end
          end

        log_by_shell(sha, options)
      end

      def fetch_source_branch!(source_repository, source_branch, local_ref)
        rugged_fetch_source_branch(source_repository, source_branch, local_ref)
      end

      private

      def uncached_has_local_branches?
        has_local_branches_rugged?
      end

      def branches_filter(filter: nil, sort_by: nil)
        branches = rugged.branches.each(filter).map do |rugged_ref|
          begin
            target_commit = Gitlab::Git::Commit.find(self, rugged_ref.target)
            Gitlab::Git::Branch.new(self, rugged_ref.name, rugged_ref.target, target_commit)
          rescue Rugged::ReferenceError
            # Omit invalid branch
          end
        end.compact

        sort_branches(branches, sort_by)
      end

      def log_by_shell(sha, options)
        limit = options[:limit].to_i
        offset = options[:offset].to_i
        use_follow_flag = options[:follow] && options[:path].present?

        # We will perform the offset in Ruby because --follow doesn't play well with --skip.
        # See: https://gitlab.com/gitlab-org/gitlab-ce/issues/3574#note_3040520
        offset_in_ruby = use_follow_flag && options[:offset].present?
        limit += offset if offset_in_ruby

        cmd = %w[log]
        cmd << "--max-count=#{limit}"
        cmd << '--format=%H'
        cmd << "--skip=#{offset}" unless offset_in_ruby
        cmd << '--follow' if use_follow_flag
        cmd << '--no-merges' if options[:skip_merges]
        cmd << "--after=#{options[:after].iso8601}" if options[:after]
        cmd << "--before=#{options[:before].iso8601}" if options[:before]

        if options[:all]
          cmd += %w[--all --reverse]
        else
          cmd << sha
        end

        # :path can be a string or an array of strings
        if options[:path].present?
          cmd << '--'
          cmd += Array(options[:path])
        end

        raw_output, _status = run_git(cmd)
        lines = offset_in_ruby ? raw_output.lines.drop(offset) : raw_output.lines

        lines.map! { |c| Rugged::Commit.new(rugged, c.strip) }
      end

      def build_git_cmd(*args)
        object_directories = alternate_object_directories.join(File::PATH_SEPARATOR)

        env = { 'PWD' => self.path }
        env['GIT_ALTERNATE_OBJECT_DIRECTORIES'] = object_directories if object_directories.present?

        [
          env,
          ::Gitlab.config.git.bin_path,
          *args,
          { chdir: self.path }
        ]
      end

      def git_diff_cmd(old_rev, new_rev)
        old_rev = old_rev == ::Gitlab::Git::BLANK_SHA ? ::Gitlab::Git::EMPTY_TREE_ID : old_rev

        build_git_cmd('diff', old_rev, new_rev, '--raw')
      end

      def git_cat_file_cmd
        format = '%(objectname) %(objectsize) %(rest)'
        build_git_cmd('cat-file', "--batch-check=#{format}")
      end

      def format_git_cat_file_script
        File.expand_path('../support/format-git-cat-file-input', __FILE__)
      end
    end
  end
end
