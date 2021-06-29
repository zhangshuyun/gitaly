require 'securerandom'

module Gitlab
  module Git
    # These are monkey patches on top of the vendored version of Repository.
    class Repository
      include Gitlab::Git::RepositoryMirroring
      include Gitlab::Git::Popen
      include Gitlab::EncodingHelper
      include Gitlab::Utils::StrongMemoize

      # In https://gitlab.com/gitlab-org/gitaly/merge_requests/698
      # We copied this prefix into gitaly-go, so don't change it
      # or things will break! (REBASE_WORKTREE_PREFIX)
      REBASE_WORKTREE_PREFIX = 'rebase'.freeze
      AM_WORKTREE_PREFIX = 'am'.freeze
      GITALY_INTERNAL_URL = 'ssh://gitaly/internal.git'.freeze
      AUTOCRLF_VALUES = { 'true' => true, 'false' => false, 'input' => :input }.freeze
      RUGGED_KEY = :rugged_list
      GIT_ALLOW_SHA_UPLOAD = 'uploadpack.allowAnySHA1InWant=true'.freeze

      NoRepository = Class.new(StandardError)
      InvalidRef = Class.new(StandardError)
      GitError = Class.new(StandardError)

      class CreateTreeError < StandardError
        attr_reader :error

        def initialize(error)
          @error = error
        end
      end

      class << self
        def from_gitaly(gitaly_repository, call)
          new(
            gitaly_repository,
            GitalyServer.repo_path(call),
            GitalyServer.gl_repository(call),
            Gitlab::Git::GitlabProjects.from_gitaly(gitaly_repository, call),
            GitalyServer.repo_alt_dirs(call),
            GitalyServer.feature_flags(call)
          )
        end

        def from_gitaly_with_block(gitaly_repository, call)
          repository = from_gitaly(gitaly_repository, call)

          result = yield repository

          repository.cleanup

          result
        end
      end

      attr_reader :path

      # Directory name of repo
      attr_reader :name

      attr_reader :gitlab_projects, :storage, :gl_repository, :gl_project_path, :relative_path

      def initialize(gitaly_repository, path, gl_repository, gitlab_projects, combined_alt_dirs = "", feature_flags = GitalyServer::FeatureFlags.new({}))
        @gitaly_repository = gitaly_repository

        @alternate_object_directories = combined_alt_dirs
                                        .split(File::PATH_SEPARATOR)
                                        .map { |d| File.join(path, d) }

        @storage = gitaly_repository.storage_name
        @relative_path = gitaly_repository.relative_path
        @path = path
        @gl_repository = gl_repository
        @gl_project_path = gitaly_repository.gl_project_path
        @gitlab_projects = gitlab_projects
        @feature_flags = feature_flags
      end

      def ==(other)
        [storage, relative_path] == [other.storage, other.relative_path]
      end

      attr_reader :gitaly_repository

      attr_reader :alternate_object_directories

      def sort_branches(branches, sort_by)
        case sort_by
        when 'name'
          branches.sort_by(&:name)
        when 'updated_desc'
          branches.sort do |a, b|
            b.dereferenced_target.committed_date <=> a.dereferenced_target.committed_date
          end
        when 'updated_asc'
          branches.sort do |a, b|
            a.dereferenced_target.committed_date <=> b.dereferenced_target.committed_date
          end
        else
          branches
        end
      end

      def exists?
        File.exist?(File.join(path, 'refs'))
      end

      def root_ref
        @root_ref ||= discover_default_branch
      end

      def rugged
        @rugged ||= begin
                      # Open in bare mode, for a slight performance gain
                      # https://github.com/libgit2/rugged/blob/654ff2fe12041e09707ba0647307abcb6348a7fb/ext/rugged/rugged_repo.c#L276-L278
                      Rugged::Repository.bare(path, alternates: alternate_object_directories).tap do |repo|
                        Thread.current[RUGGED_KEY] << repo if Thread.current[RUGGED_KEY]
                      end
                    end
      rescue Rugged::RepositoryError, Rugged::OSError
        raise NoRepository, 'no repository for such path'
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

      # Git repository can contains some hidden refs like:
      #   /refs/notes/*
      #   /refs/git-as-svn/*
      #   /refs/pulls/*
      # This refs by default not visible in project page and not cloned to client side.
      def has_visible_content?
        strong_memoize(:has_visible_content) do
          branches_filter(filter: :local).any? do |ref|
            begin
              ref.name && ref.target # ensures the branch is valid

              true
            rescue Rugged::ReferenceError
              false
            end
          end
        end
      end

      def tags
        rugged.references.each("refs/tags/*").map do |ref|
          message = nil

          if ref.target.is_a?(Rugged::Tag::Annotation)
            tag_message = ref.target.message

            message = tag_message.chomp if tag_message.respond_to?(:chomp)
          end

          target_commit = Gitlab::Git::Commit.find(self, ref.target)
          Gitlab::Git::Tag.new(self,
                               name: ref.canonical_name,
                               target: ref.target,
                               target_commit: target_commit,
                               message: message)
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

      def ancestor?(from, to)
        return false if from.nil? || to.nil?

        merge_base(from, to) == from
      rescue Rugged::OdbError
        false
      end

      def diff_exists?(sha1, sha2)
        rugged.diff(sha1, sha2).size.positive?
      end

      # rubocop:disable Metrics/ParameterLists
      def rebase(user, rebase_id, branch:, branch_sha:, remote_repository:, remote_branch:, push_options: nil, timestamp: nil, transaction: nil)
        worktree = Gitlab::Git::Worktree.new(path, REBASE_WORKTREE_PREFIX, rebase_id)
        env = user.git_env(timestamp)

        with_repo_branch_commit(remote_repository, remote_branch) do |commit|
          diff_range = "#{commit.sha}...#{branch}"
          diff_files = begin
                         run_git!(
                           %W[diff --name-only #{diff_range}]
                         ).chomp
                       rescue GitError
                         []
                       end

          with_worktree(worktree, branch, sparse_checkout_files: diff_files, env: env) do
            run_git!(
              %W[rebase #{commit.sha}],
              chdir: worktree.path, env: env, include_stderr: true
            )

            rebase_sha = run_git!(%w[rev-parse HEAD], chdir: worktree.path, env: env).strip

            yield rebase_sha if block_given?

            update_branch(branch, user: user, newrev: rebase_sha, oldrev: branch_sha, push_options: push_options, transaction: transaction)

            rebase_sha
          end
        end
      end
      # rubocop:enable Metrics/ParameterLists

      def commit_patches(start_point, patches, extra_env: {})
        worktree = Gitlab::Git::Worktree.new(path, AM_WORKTREE_PREFIX, SecureRandom.hex)

        with_worktree(worktree, start_point, env: extra_env) do
          result, status = run_git(%w[am --quiet --3way], chdir: worktree.path, env: extra_env) do |stdin|
            loop { stdin.write(patches.next) }
          end

          raise Gitlab::Git::PatchError, result unless status == 0

          run_git!(
            %w[rev-parse --quiet --verify HEAD], chdir: worktree.path, env: extra_env
          ).chomp
        end
      end

      def update_submodule(submodule_path, commit_sha, branch, committer, message)
        target = rugged.rev_parse("refs/heads/" + branch)
        raise CommitError, 'Invalid branch' unless target.is_a?(Rugged::Commit)

        current_entry = rugged_submodule_entry(target, submodule_path)
        raise CommitError, 'Invalid submodule path' unless current_entry
        raise CommitError, "The submodule #{submodule_path} is already at #{commit_sha}" if commit_sha == current_entry[:oid]

        commit_tree = target.tree.update([action: :upsert,
                                          oid: commit_sha,
                                          filemode: 0o160000,
                                          path: submodule_path])

        options = {
          parents: [target.oid],
          tree: commit_tree,
          message: message,
          author: committer,
          committer: committer
        }

        create_commit(options).tap do |result|
          raise CommitError, 'Failed to create commit' unless result
        end
      end

      def with_repo_branch_commit(start_repository, start_ref)
        start_repository = RemoteRepository.new(start_repository) unless start_repository.is_a?(RemoteRepository)

        if start_repository.empty?
          return yield nil
        elsif start_repository.same_repository?(self)
          # Directly return the commit from this repository
          return yield commit(start_ref)
        end

        # Find the commit from the remote repository (this triggers an RPC)
        commit_id = start_repository.commit_id(start_ref)
        return yield nil unless commit_id

        if existing_commit = commit(commit_id)
          # Commit is already present (e.g. in a fork, or through a previous fetch)
          yield existing_commit
        else
          fetch_sha(start_repository, commit_id)
          yield commit(commit_id)
        end
      end

      # Directly find a branch with a simple name (e.g. master)
      #
      # force_reload causes a new Rugged repository to be instantiated
      #
      # This is to work around a bug in libgit2 that causes in-memory refs to
      # be stale/invalid when packed-refs is changed.
      # See https://gitlab.com/gitlab-org/gitlab-ce/issues/15392#note_14538333
      def find_branch(name, force_reload = false)
        reload_rugged if force_reload

        rugged_ref = rugged.ref("refs/heads/" + name)
        if rugged_ref
          target_commit = Gitlab::Git::Commit.find(self, rugged_ref.target)
          Gitlab::Git::Branch.new(self, rugged_ref.canonical_name, rugged_ref.target, target_commit)
        end
      end

      def delete_refs(*ref_names)
        git_delete_refs(*ref_names)
      end

      # Returns true if the given branch exists
      #
      # name - The name of the branch as a String.
      def branch_exists?(name)
        rugged.branches.exists?(name)

      # If the branch name is invalid (e.g. ".foo") Rugged will raise an error.
      # Whatever code calls this method shouldn't have to deal with that so
      # instead we just return `false` (which is true since a branch doesn't
      # exist when it has an invalid name).
      rescue Rugged::ReferenceError
        false
      end

      def merge_base(from, to)
        rugged.merge_base(from, to)
      rescue Rugged::ReferenceError
        nil
      end

      def user_to_committer(user, timestamp = nil)
        Gitlab::Git.committer_hash(email: user.email, name: user.name, timestamp: timestamp)
      end

      # Fetch a commit from the given source repository
      def fetch_sha(source_repository, sha)
        source_repository = RemoteRepository.new(source_repository) unless source_repository.is_a?(RemoteRepository)

        env = source_repository.fetch_env(git_config_options: [GIT_ALLOW_SHA_UPLOAD])

        args = %W[fetch --no-tags #{GITALY_INTERNAL_URL} #{sha}]
        message, status = run_git(args, env: env, include_stderr: true)
        raise Gitlab::Git::CommandError, message unless status.zero?

        sha
      end

      # Lookup for rugged object by oid or ref name
      def lookup(oid_or_ref_name)
        rugged.rev_parse(oid_or_ref_name)
      end

      def commit_index(user, branch_name, index, options, timestamp = nil)
        committer = user_to_committer(user, timestamp)

        OperationService.new(user, self).with_branch(branch_name) do
          commit_params = options.merge(
            tree: index.write_tree(rugged),
            author: committer,
            committer: committer
          )

          create_commit(commit_params)
        end
      end

      # Return the object that +revspec+ points to.  If +revspec+ is an
      # annotated tag, then return the tag's target instead.
      def rev_parse_target(revspec)
        obj = rugged.rev_parse(revspec)
        Ref.dereference_object(obj)
      end

      def add_remote(remote_name, url, mirror_refmap: nil)
        rugged.remotes.create(remote_name, url)

        set_remote_as_mirror(remote_name, refmap: mirror_refmap) if mirror_refmap
      rescue Rugged::ConfigError
        remote_update(remote_name, url: url)
      end

      # Update the specified remote using the values in the +options+ hash
      #
      # Example
      # repo.update_remote("origin", url: "path/to/repo")
      def remote_update(remote_name, url:)
        # TODO: Implement other remote options
        rugged.remotes.set_url(remote_name, url)
        nil
      end

      def commit(ref = nil)
        ref ||= root_ref
        Gitlab::Git::Commit.find(self, ref)
      end

      def empty?
        !has_visible_content?
      end

      def autocrlf
        AUTOCRLF_VALUES[rugged.config['core.autocrlf']]
      end

      def autocrlf=(value)
        rugged.config['core.autocrlf'] = AUTOCRLF_VALUES.invert[value]
      end

      def blob_at(sha, path)
        Gitlab::Git::Blob.find(self, sha, path) unless Gitlab::Git.blank_ref?(sha)
      end

      def cleanup
        # Opening a repository may be expensive, and we only need to close it
        # if it's been open.
        rugged&.close if defined?(@rugged)
      end

      def head_symbolic_ref
        message, status = run_git(%w[symbolic-ref HEAD])

        return 'main' if status.nonzero?

        Ref.extract_branch_name(message.squish)
      end

      private

      def sparse_checkout_empty?(output)
        output.include?("error: Sparse checkout leaves no entry on working directory")
      end

      def disable_sparse_checkout
        run_git!(%w[config core.sparseCheckout false], include_stderr: true)
      end

      def run_git(args, chdir: path, env: {}, nice: false, include_stderr: false, lazy_block: nil, &block)
        cmd = [Gitlab.config.git.bin_path, *args]
        cmd.unshift("nice") if nice

        object_directories = alternate_object_directories
        env['GIT_ALTERNATE_OBJECT_DIRECTORIES'] = object_directories.join(File::PATH_SEPARATOR) if object_directories.any?

        popen(cmd, chdir, env, include_stderr: include_stderr, lazy_block: lazy_block, &block)
      end

      def run_git!(args, chdir: path, env: {}, nice: false, include_stderr: false, lazy_block: nil, &block)
        output, status = run_git(args, chdir: chdir, env: env, nice: nice, include_stderr: include_stderr, lazy_block: lazy_block, &block)

        raise GitError, output unless status.zero?

        output
      end

      def branches_filter(filter: nil, sort_by: nil)
        branches = rugged.branches.each(filter).map do |rugged_ref|
          begin
            target_commit = Gitlab::Git::Commit.find(self, rugged_ref.target)
            Gitlab::Git::Branch.new(self, rugged_ref.canonical_name, rugged_ref.target, target_commit)
          rescue Rugged::ReferenceError
            # Omit invalid branch
          end
        end.compact

        sort_branches(branches, sort_by)
      end

      def git_delete_refs(*ref_names)
        instructions = ref_names.map do |ref|
          "delete #{ref}\x00\x00"
        end

        message, status = run_git(%w[update-ref --stdin -z], include_stderr: true) do |stdin|
          stdin.write(instructions.join)
        end

        raise GitError, "Could not delete refs #{ref_names}: #{message}" unless status.zero?
      end

      def create_commit(params = {})
        params[:message].delete!("\r")

        Rugged::Commit.create(rugged, params)
      end

      def rugged_head
        rugged.head
      rescue Rugged::ReferenceError
        nil
      end

      def with_worktree(worktree, branch, sparse_checkout_files: nil, env:)
        base_args = %w[worktree add --detach]

        run_git!(%w[config core.splitIndex false])

        # Note that we _don't_ want to test for `.present?` here: If the caller
        # passes an non nil empty value it means it still wants sparse checkout
        # but just isn't interested in any file, perhaps because it wants to
        # checkout files in by a changeset but that changeset only adds files.
        if sparse_checkout_files
          # Create worktree without checking out
          run_git!(base_args + ['--no-checkout', worktree.path], env: env, include_stderr: true)
          worktree_git_path = run_git!(%w[rev-parse --git-dir], chdir: worktree.path).chomp

          configure_sparse_checkout(worktree_git_path, sparse_checkout_files)

          # After sparse checkout configuration, checkout `branch` in worktree
          output, cmd_status = run_git(%W[checkout --detach #{branch}], chdir: worktree.path, env: env, include_stderr: true)

          # If sparse checkout fails, fall back to a regular checkout.
          if cmd_status.nonzero?
            if sparse_checkout_empty?(output)
              disable_sparse_checkout
              run_git!(%W[checkout --detach #{branch}], chdir: worktree.path, env: env, include_stderr: true)
            else
              raise GitError, output
            end
          end
        else
          # Create worktree and checkout `branch` in it
          run_git!(base_args + [worktree.path, branch], env: env, include_stderr: true)
        end

        yield
      ensure
        run_git(%W[worktree remove -f #{worktree.name}], include_stderr: true)
      end

      # Adding a worktree means checking out the repository. For large repos,
      # this can be very expensive, so set up sparse checkout for the worktree
      # to only check out the files we're interested in.
      def configure_sparse_checkout(worktree_git_path, files)
        run_git!(%w[config core.sparseCheckout true], include_stderr: true)

        return if files.empty?

        worktree_info_path = File.join(worktree_git_path, 'info')
        FileUtils.mkdir_p(worktree_info_path)
        File.write(File.join(worktree_info_path, 'sparse-checkout'), files)
      end

      def gitlab_projects_error
        raise CommandError, @gitlab_projects.output
      end

      def rugged_submodule_entry(target, submodule_path)
        parent_dir = File.dirname(submodule_path)
        parent_dir = '' if parent_dir == '.'
        parent_tree = rugged.rev_parse("#{target.oid}^{tree}:#{parent_dir}")

        return unless parent_tree.is_a?(Rugged::Tree)

        current_entry = parent_tree[File.basename(submodule_path)]

        valid_submodule_entry?(current_entry) ? current_entry : nil
      end

      def valid_submodule_entry?(entry)
        entry && entry[:type] == :commit
      end
    end
  end
end
