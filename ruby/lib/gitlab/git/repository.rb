module Gitlab
  module Git
    # These are monkey patches on top of the vendored version of Repository.
    class Repository
      include Gitlab::Git::RepositoryMirroring
      include Gitlab::Git::Popen
      include Gitlab::EncodingHelper
      include Gitlab::Utils::StrongMemoize

      ALLOWED_OBJECT_DIRECTORIES_VARIABLES = %w[
        GIT_OBJECT_DIRECTORY
        GIT_ALTERNATE_OBJECT_DIRECTORIES
      ].freeze
      ALLOWED_OBJECT_RELATIVE_DIRECTORIES_VARIABLES = %w[
        GIT_OBJECT_DIRECTORY_RELATIVE
        GIT_ALTERNATE_OBJECT_DIRECTORIES_RELATIVE
      ].freeze
      SEARCH_CONTEXT_LINES = 3
      REV_LIST_COMMIT_LIMIT = 2_000
      # In https://gitlab.com/gitlab-org/gitaly/merge_requests/698
      # We copied these two prefixes into gitaly-go, so don't change these
      # or things will break! (REBASE_WORKTREE_PREFIX and SQUASH_WORKTREE_PREFIX)
      REBASE_WORKTREE_PREFIX = 'rebase'.freeze
      SQUASH_WORKTREE_PREFIX = 'squash'.freeze
      GITALY_INTERNAL_URL = 'ssh://gitaly/internal.git'.freeze
      GITLAB_PROJECTS_TIMEOUT = Gitlab.config.gitlab_shell.git_timeout
      EMPTY_REPOSITORY_CHECKSUM = '0000000000000000000000000000000000000000'.freeze
      AUTOCRLF_VALUES = { 'true' => true, 'false' => false, 'input' => :input }.freeze
      RUGGED_KEY = :rugged_list

      NoRepository = Class.new(StandardError)
      InvalidRepository = Class.new(StandardError)
      InvalidBlobName = Class.new(StandardError)
      InvalidRef = Class.new(StandardError)
      GitError = Class.new(StandardError)
      DeleteBranchError = Class.new(StandardError)
      CreateTreeError = Class.new(StandardError)
      TagExistsError = Class.new(StandardError)
      ChecksumError = Class.new(StandardError)

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

        def from_gitaly_with_block(gitaly_repository, call)
          repository = from_gitaly(gitaly_repository, call)

          yield repository

          repository.cleanup
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

      # Directory name of repo
      attr_reader :name

      # Relative path of repo
      attr_reader :relative_path

      attr_reader :gitlab_projects, :storage, :gl_repository, :relative_path

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

      def ==(other)
        [storage, relative_path] == [other.storage, other.relative_path]
      end

      def add_branch(branch_name, user:, target:)
        target_object = Ref.dereference_object(lookup(target))
        raise InvalidRef.new("target not found: #{target}") unless target_object

        OperationService.new(user, self).add_branch(branch_name, target_object.oid)
        find_branch(branch_name)
      rescue Rugged::ReferenceError => ex
        raise InvalidRef, ex
      end

      def gitaly_repository
        @gitaly_repository
      end

      def alternate_object_directories
        @alternate_object_directories
      end

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

      def rugged
        @rugged ||= begin
                      Rugged::Repository.new(path, alternates: alternate_object_directories).tap do |repo|
                        Thread.current[RUGGED_KEY] << repo if Thread.current[RUGGED_KEY]
                      end
                    end
      rescue Rugged::RepositoryError, Rugged::OSError
        raise NoRepository.new('no repository for such path')
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

        merge_base(from, to) == from
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

            result = []

            Open3.popen3(*git_diff_cmd(old_rev, new_rev)) do |stdin, stdout, _stderr, wait_thr|
              cat_stdin, cat_stdout, cat_stderr, cat_wait_thr = Open3.popen3(*git_cat_file_cmd)

              stdout.each_line do |line|
                old_mode, new_mode, blob_id, rest = parse_raw_diff_line(line)
                cat_stdin.puts("#{blob_id} #{rest}")
                result << ::Gitlab::Git::RawDiffChange.new(cat_stdout.gets.chomp, old_mode, new_mode)
              end

              cat_stdin.close
              cat_stdout.close
              cat_stderr.close

              unless [cat_wait_thr, wait_thr].all? { |waiter| waiter.value&.success? }
                raise ::Gitlab::Git::Repository::GitError, "Unabled to obtain changes between #{old_rev} and #{new_rev}"
              end
            end

            result
          end
      rescue ArgumentError => e
        raise Gitlab::Git::Repository::GitError.new(e.to_s)
      end

      def parse_raw_diff_line(line)
        old_mode, new_mode, old_blob_id, new_blob_id, rest = line.split(/\s/, 5)

        # If the last element got a value we should be good
        raise ArgumentError, "Invalid diff line: #{line}" unless rest

        old_mode.gsub!(/\A:/, '')
        old_blob_id.gsub!(/[^\h]/, '')
        new_blob_id.gsub!(/[^\h]/, '')

        # We can't pass '0000000...' to `git cat-file` given it will not return info about the deleted file
        blob_id = new_blob_id =~ /\A0+\z/ ? old_blob_id : new_blob_id

        [old_mode, new_mode, blob_id, rest]
      end

      def add_tag(tag_name, user:, target:, message: nil)
        target_object = Ref.dereference_object(lookup(target))
        raise InvalidRef.new("target not found: #{target}") unless target_object

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

      def update_branch(branch_name, user:, newrev:, oldrev:)
        OperationService.new(user, self).update_branch(branch_name, newrev, oldrev)
      end

      def rm_branch(branch_name, user:)
        branch = find_branch(branch_name)

        raise InvalidRef.new("branch not found: #{branch_name}") unless branch

        OperationService.new(user, self).rm_branch(branch)
      end

      def rm_tag(tag_name, user:)
        tag = find_tag(tag_name)

        raise InvalidRef.new("tag not found: #{tag_name}") unless tag

        Gitlab::Git::OperationService.new(user, self).rm_tag(tag)
      end

      def find_tag(name)
        tags.find { |tag| tag.name == name }
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

      def diff_exists?(sha1, sha2)
        rugged.diff(sha1, sha2).size > 0
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

      def push_remote_branches(remote_name, branch_names, forced: true)
        success = @gitlab_projects.push_branches(remote_name, GITLAB_PROJECTS_TIMEOUT, forced, branch_names)

        success || gitlab_projects_error
      end

      def delete_remote_branches(remote_name, branch_names)
        success = @gitlab_projects.delete_remote_branches(remote_name, branch_names)

        success || gitlab_projects_error
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

      def with_repo_branch_commit(start_repository, start_branch_name)
        Gitlab::Git.check_namespace!(start_repository)
        start_repository = RemoteRepository.new(start_repository) unless start_repository.is_a?(RemoteRepository)

        return yield nil if start_repository.empty?

        if start_repository.same_repository?(self)
          yield commit(start_branch_name)
        else
          start_commit_id = start_repository.commit_id(start_branch_name)

          return yield nil unless start_commit_id

          if branch_commit = commit(start_commit_id)
            yield branch_commit
          else
            with_repo_tmp_commit(
              start_repository, start_branch_name, start_commit_id) do |tmp_commit|
              yield tmp_commit
            end
          end
        end
      end

      def with_repo_tmp_commit(start_repository, start_branch_name, sha)
        source_ref = start_branch_name

        unless Gitlab::Git.branch_ref?(source_ref)
          source_ref = "#{Gitlab::Git::BRANCH_REF_PREFIX}#{source_ref}"
        end

        tmp_ref = fetch_ref(
          start_repository,
          source_ref: source_ref,
          target_ref: "refs/tmp/#{SecureRandom.hex}"
        )

        yield commit(sha)
      ensure
        delete_refs(tmp_ref) if tmp_ref
      end

      def fetch_source_branch!(source_repository, source_branch, local_ref)
        rugged_fetch_source_branch(source_repository, source_branch, local_ref)
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

        rugged_ref = rugged.branches[name]
        if rugged_ref
          target_commit = Gitlab::Git::Commit.find(self, rugged_ref.target)
          Gitlab::Git::Branch.new(self, rugged_ref.name, rugged_ref.target, target_commit)
        end
      end

      # Delete the specified branch from the repository
      def delete_branch(branch_name)
        rugged.branches.delete(branch_name)
      rescue Rugged::ReferenceError => e
        raise DeleteBranchError, e
      end

      def delete_refs(*ref_names)
        git_delete_refs(*ref_names)
      end

      def delete_all_refs_except(prefixes)
        delete_refs(*all_ref_names_except(prefixes))
      end

      # Returns an Array of all ref names, except when it's matching pattern
      #
      # regexp - The pattern for ref names we don't want
      def all_ref_names_except(prefixes)
        rugged.references.reject do |ref|
          prefixes.any? { |p| ref.name.start_with?(p) }
        end.map(&:name)
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

      def user_to_committer(user)
        Gitlab::Git.committer_hash(email: user.email, name: user.name)
      end

      def write_ref(ref_path, ref, old_ref: nil, shell: true)
        if shell
          shell_write_ref(ref_path, ref, old_ref)
        else
          rugged_write_ref(ref_path, ref)
        end
      end

      def fetch_ref(source_repository, source_ref:, target_ref:)
        Gitlab::Git.check_namespace!(source_repository)
        source_repository = RemoteRepository.new(source_repository) unless source_repository.is_a?(RemoteRepository)

        args = %W(fetch --no-tags -f #{GITALY_INTERNAL_URL} #{source_ref}:#{target_ref})
        message, status = run_git(args, env: source_repository.fetch_env)
        raise Gitlab::Git::CommandError, message if status != 0

        target_ref
      end

      # Lookup for rugged object by oid or ref name
      def lookup(oid_or_ref_name)
        rugged.rev_parse(oid_or_ref_name)
      end

      def commit_index(user, branch_name, index, options)
        committer = user_to_committer(user)

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

      def remove_remote(remote_name)
        # When a remote is deleted all its remote refs are deleted too, but in
        # the case of mirrors we map its refs (that would usualy go under
        # [remote_name]/) to the top level namespace. We clean the mapping so
        # those don't get deleted.
        if rugged.config["remote.#{remote_name}.mirror"]
          rugged.config.delete("remote.#{remote_name}.fetch")
        end

        rugged.remotes.delete(remote_name)
        true
      rescue Rugged::ConfigError
        false
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

      def commit(ref = 'HEAD')
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

      def fetch_repository_as_mirror(repository)
        remote_name = "tmp-#{SecureRandom.hex}"
        repository = RemoteRepository.new(repository) unless repository.is_a?(RemoteRepository)

        add_remote(remote_name, GITALY_INTERNAL_URL, mirror_refmap: :all_refs)
        fetch_remote(remote_name, env: repository.fetch_env)
      ensure
        remove_remote(remote_name)
      end

      def fetch_remote(remote_name = 'origin', env: nil)
        run_git(['fetch', remote_name], env: env).last.zero?
      end

      def rev_list(including: [], excluding: [], options: [], objects: false, &block)
        args = ['rev-list']

        args.push(*rev_list_param(including))

        exclude_param = *rev_list_param(excluding)
        if exclude_param.any?
          args.push('--not')
          args.push(*exclude_param)
        end

        args.push('--objects') if objects

        if options.any?
          args.push(*options)
        end

        run_git!(args, lazy_block: block)
      end

      def cleanup
        # Opening a repository may be expensive, and we only need to close it
        # if it's been open.
        rugged&.close if defined?(@rugged)
      end

      private

      def run_git(args, chdir: path, env: {}, nice: false, lazy_block: nil, &block)
        cmd = [Gitlab.config.git.bin_path, *args]
        cmd.unshift("nice") if nice

        object_directories = alternate_object_directories
        if object_directories.any?
          env['GIT_ALTERNATE_OBJECT_DIRECTORIES'] = object_directories.join(File::PATH_SEPARATOR)
        end

        popen(cmd, chdir, env, lazy_block: lazy_block, &block)
      end

      def run_git!(args, chdir: path, env: {}, nice: false, lazy_block: nil, &block)
        output, status = run_git(args, chdir: chdir, env: env, nice: nice, lazy_block: lazy_block, &block)

        raise GitError, output unless status.zero?

        output
      end

      def run_git_with_timeout(args, timeout, env: {})
        popen_with_timeout([Gitlab.config.git.bin_path, *args], timeout, path, env)
      end

      def git_env_for_user(user)
        {
          'GIT_COMMITTER_NAME' => user.name,
          'GIT_COMMITTER_EMAIL' => user.email,
          'GL_ID' => Gitlab::GlId.gl_id(user),
          'GL_PROTOCOL' => Gitlab::Git::Hook::GL_PROTOCOL,
          'GL_REPOSITORY' => gl_repository
        }
      end

      def check_revert_content(target_commit, source_sha)
        args = [target_commit.sha, source_sha]
        args << { mainline: 1 } if target_commit.merge_commit?

        revert_index = rugged.revert_commit(*args)
        return false if revert_index.conflicts?

        tree_id = revert_index.write_tree(rugged)
        return false unless diff_exists?(source_sha, tree_id)

        tree_id
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

      def git_delete_refs(*ref_names)
        instructions = ref_names.map do |ref|
          "delete #{ref}\x00\x00"
        end

        message, status = run_git(%w[update-ref --stdin -z]) do |stdin|
          stdin.write(instructions.join)
        end

        unless status.zero?
          raise GitError.new("Could not delete refs #{ref_names}: #{message}")
        end
      end

      def rugged_cherry_pick(user:, commit:, branch_name:, message:, start_branch_name:, start_repository:)
        OperationService.new(user, self).with_branch(
          branch_name,
          start_branch_name: start_branch_name,
          start_repository: start_repository
        ) do |start_commit|

          Gitlab::Git.check_namespace!(commit, start_repository)

          cherry_pick_tree_id = check_cherry_pick_content(commit, start_commit.sha)
          raise CreateTreeError unless cherry_pick_tree_id

          committer = user_to_committer(user)

          create_commit(message: message,
                        author: {
                            email: commit.author_email,
                            name: commit.author_name,
                            time: commit.authored_date
                        },
                        committer: committer,
                        tree: cherry_pick_tree_id,
                        parents: [start_commit.sha])
        end
      end

      def check_cherry_pick_content(target_commit, source_sha)
        args = [target_commit.sha, source_sha]
        args << 1 if target_commit.merge_commit?

        cherry_pick_index = rugged.cherrypick_commit(*args)
        return false if cherry_pick_index.conflicts?

        tree_id = cherry_pick_index.write_tree(rugged)
        return false unless diff_exists?(source_sha, tree_id)

        tree_id
      end

      def create_commit(params = {})
        params[:message].delete!("\r")

        Rugged::Commit.create(rugged, params)
      end

      def shell_write_ref(ref_path, ref, old_ref)
        raise ArgumentError, "invalid ref_path #{ref_path.inspect}" if ref_path.include?(' ')
        raise ArgumentError, "invalid ref #{ref.inspect}" if ref.include?("\x00")
        raise ArgumentError, "invalid old_ref #{old_ref.inspect}" if !old_ref.nil? && old_ref.include?("\x00")

        input = "update #{ref_path}\x00#{ref}\x00#{old_ref}\x00"
        run_git!(%w[update-ref --stdin -z]) { |stdin| stdin.write(input) }
      end

      def rugged_write_ref(ref_path, ref)
        rugged.references.create(ref_path, ref, force: true)
      rescue Rugged::ReferenceError => ex
        Rails.logger.error "Unable to create #{ref_path} reference for repository #{path}: #{ex}"
      rescue Rugged::OSError => ex
        raise unless ex.message =~ /Failed to create locked file/ && ex.message =~ /File exists/

        Rails.logger.error "Unable to create #{ref_path} reference for repository #{path}: #{ex}"
      end

      def rugged_head
        rugged.head
      rescue Rugged::ReferenceError
        nil
      end

      def with_worktree(worktree_path, branch, sparse_checkout_files: nil, env:)
        base_args = %w(worktree add --detach)

        # Note that we _don't_ want to test for `.present?` here: If the caller
        # passes an non nil empty value it means it still wants sparse checkout
        # but just isn't interested in any file, perhaps because it wants to
        # checkout files in by a changeset but that changeset only adds files.
        if sparse_checkout_files
          # Create worktree without checking out
          run_git!(base_args + ['--no-checkout', worktree_path], env: env)
          worktree_git_path = run_git!(%w(rev-parse --git-dir), chdir: worktree_path).chomp

          configure_sparse_checkout(worktree_git_path, sparse_checkout_files)

          # After sparse checkout configuration, checkout `branch` in worktree
          run_git!(%W(checkout --detach #{branch}), chdir: worktree_path, env: env)
        else
          # Create worktree and checkout `branch` in it
          run_git!(base_args + [worktree_path, branch], env: env)
        end

        yield
      ensure
        FileUtils.rm_rf(worktree_path) if File.exist?(worktree_path)
        FileUtils.rm_rf(worktree_git_path) if worktree_git_path && File.exist?(worktree_git_path)
      end

      # Adding a worktree means checking out the repository. For large repos,
      # this can be very expensive, so set up sparse checkout for the worktree
      # to only check out the files we're interested in.
      def configure_sparse_checkout(worktree_git_path, files)
        run_git!(%w(config core.sparseCheckout true))

        return if files.empty?

        worktree_info_path = File.join(worktree_git_path, 'info')
        FileUtils.mkdir_p(worktree_info_path)
        File.write(File.join(worktree_info_path, 'sparse-checkout'), files)
      end

      def worktree_path(prefix, id)
        id = id.to_s
        raise ArgumentError, "worktree id can't be empty" unless id.present?
        raise ArgumentError, "worktree id can't contain slashes " if id.include?("/")

        File.join(path, 'gitlab-worktree', "#{prefix}-#{id}")
      end

      def rugged_fetch_source_branch(source_repository, source_branch, local_ref)
        with_repo_branch_commit(source_repository, source_branch) do |commit|
          if commit
            write_ref(local_ref, commit.sha)
            true
          else
            false
          end
        end
      end

      def sha_from_ref(ref)
        rev_parse_target(ref).oid
      end

      def gitlab_projects_error
        raise CommandError, @gitlab_projects.output
      end

      def rev_list_param(spec)
        spec == :all ? ['--all'] : spec
      end
    end
  end
end
