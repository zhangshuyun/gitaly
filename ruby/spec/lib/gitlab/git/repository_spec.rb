require 'spec_helper'

describe Gitlab::Git::Repository do # rubocop:disable Metrics/BlockLength
  include TestRepo
  include Gitlab::EncodingHelper
  using RSpec::Parameterized::TableSyntax

  let(:mutable_repository) { gitlab_git_from_gitaly(new_mutable_git_test_repo) }
  let(:repository) { gitlab_git_from_gitaly(git_test_repo_read_only) }
  let(:repository_path) { repository.path }
  let(:repository_rugged) { Rugged::Repository.new(repository_path) }
  let(:storage_path) { DEFAULT_STORAGE_DIR }
  let(:user) { Gitlab::Git::User.new('johndone', 'John Doe', 'johndoe@mail.com', 'user-1') }

  describe '.from_gitaly_with_block' do
    let(:call_metadata) do
      {
        'user-agent' => 'grpc-go/1.9.1',
        'gitaly-storage-path' => DEFAULT_STORAGE_DIR,
        'gitaly-repo-path' => TEST_REPO_PATH,
        'gitaly-gl-repository' => 'project-52',
        'gitaly-repo-alt-dirs' => ''
      }
    end
    let(:call) { double(metadata: call_metadata) }

    it 'cleans up the repository' do
      described_class.from_gitaly_with_block(test_repo_read_only, call) do |repository|
        expect(repository.rugged).to receive(:close)
      end
    end

    it 'returns the passed result of the block passed' do
      result = described_class.from_gitaly_with_block(test_repo_read_only, call) { 'Hello world' }

      expect(result).to eq('Hello world')
    end
  end

  describe "Respond to" do
    subject { repository }

    it { is_expected.to respond_to(:root_ref) }
    it { is_expected.to respond_to(:tags) }
  end

  describe '#root_ref' do
    it 'calls #discover_default_branch' do
      expect(repository).to receive(:discover_default_branch)
      repository.root_ref
    end
  end

  describe '#branch_names' do
    subject { repository.branch_names }

    it 'has SeedRepo::Repo::BRANCHES.size elements' do
      expect(subject.size).to eq(SeedRepo::Repo::BRANCHES.size)
    end

    it { is_expected.to include("master") }
    it { is_expected.not_to include("branch-from-space") }
  end

  describe '#tags' do
    describe 'first tag' do
      let(:tag) { repository.tags.first }

      it { expect(tag.name).to eq("v1.0.0") }
      it { expect(tag.target).to eq("f4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8") }
      it { expect(tag.dereferenced_target.sha).to eq("6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9") }
      it { expect(tag.message).to eq("Release") }
    end

    describe 'last tag' do
      let(:tag) { repository.tags.last }

      it { expect(tag.name).to eq("v1.2.1") }
      it { expect(tag.target).to eq("2ac1f24e253e08135507d0830508febaaccf02ee") }
      it { expect(tag.dereferenced_target.sha).to eq("fa1b1e6c004a68b7d8763b86455da9e6b23e36d6") }
      it { expect(tag.message).to eq("Version 1.2.1") }
    end

    it { expect(repository.tags.size).to eq(SeedRepo::Repo::TAGS.size) }
  end

  describe '#empty?' do
    it { expect(repository).not_to be_empty }
  end

  describe "#delete_branch" do
    let(:repository) { mutable_repository }

    it "removes the branch from the repo" do
      branch_name = "to-be-deleted-soon"

      create_branch(repository, branch_name)
      expect(repository_rugged.branches[branch_name]).not_to be_nil

      repository.delete_branch(branch_name)
      expect(repository_rugged.branches[branch_name]).to be_nil
    end

    context "when branch does not exist" do
      it "raises a DeleteBranchError exception" do
        expect { repository.delete_branch("this-branch-does-not-exist") }.to raise_error(Gitlab::Git::Repository::DeleteBranchError)
      end
    end
  end

  describe '#delete_refs' do
    let(:repository) { mutable_repository }

    it 'deletes the ref' do
      repository.delete_refs('refs/heads/feature')

      expect(repository_rugged.references['refs/heads/feature']).to be_nil
    end

    it 'deletes all refs' do
      refs = %w[refs/heads/wip refs/tags/v1.1.0]
      repository.delete_refs(*refs)

      refs.each do |ref|
        expect(repository_rugged.references[ref]).to be_nil
      end
    end

    it 'does not fail when deleting an empty list of refs' do
      expect { repository.delete_refs }.not_to raise_error
    end

    it 'raises an error if it failed' do
      expect { repository.delete_refs('refs\heads\fix') }.to raise_error(Gitlab::Git::Repository::GitError)
    end
  end

  describe '#fetch_repository_as_mirror' do
    let(:new_repository) { gitlab_git_from_gitaly(new_empty_test_repo) }
    let(:repository) { mutable_repository }
    let(:remote_repository) { Gitlab::Git::RemoteRepository.new(repository) }
    let(:fake_env) { {} }

    subject { new_repository.fetch_repository_as_mirror(remote_repository) }

    it 'fetches a repository as a mirror remote' do
      expect(new_repository).to receive(:add_remote).with(anything, Gitlab::Git::Repository::GITALY_INTERNAL_URL, mirror_refmap: :all_refs)
      expect(remote_repository).to receive(:fetch_env).and_return(fake_env)
      expect(new_repository).to receive(:fetch_remote).with(anything, env: fake_env)
      expect(new_repository).to receive(:remove_remote).with(anything)

      subject
    end
  end

  describe '#merge_base' do
    where(:from, :to, :result) do
      '570e7b2abdd848b95f2f578043fc23bd6f6fd24d' | '40f4a7a617393735a95a0bb67b08385bc1e7c66d' | '570e7b2abdd848b95f2f578043fc23bd6f6fd24d'
      '40f4a7a617393735a95a0bb67b08385bc1e7c66d' | '570e7b2abdd848b95f2f578043fc23bd6f6fd24d' | '570e7b2abdd848b95f2f578043fc23bd6f6fd24d'
      '40f4a7a617393735a95a0bb67b08385bc1e7c66d' | 'foobar' | nil
      'foobar' | '40f4a7a617393735a95a0bb67b08385bc1e7c66d' | nil
    end

    with_them do
      it { expect(repository.merge_base(from, to)).to eq(result) }
    end
  end

  describe '#find_branch' do
    it 'should return a Branch for master' do
      branch = repository.find_branch('master')

      expect(branch).to be_a_kind_of(Gitlab::Git::Branch)
      expect(branch.name).to eq('master')
    end

    it 'should handle non-existent branch' do
      branch = repository.find_branch('this-is-garbage')

      expect(branch).to eq(nil)
    end
  end

  describe '#branches' do
    subject { repository.branches }

    context 'with local and remote branches' do
      let(:repository) { mutable_repository }

      before do
        create_remote_branch('joe', 'remote_branch', 'master')
        create_branch(repository, 'local_branch', 'master')
      end

      it 'returns the local and remote branches' do
        expect(subject.any? { |b| b.name == 'joe/remote_branch' }).to eq(true)
        expect(subject.any? { |b| b.name == 'local_branch' }).to eq(true)
      end
    end
  end

  describe '#branch_exists?' do
    it 'returns true for an existing branch' do
      expect(repository.branch_exists?('master')).to eq(true)
    end

    it 'returns false for a non-existing branch' do
      expect(repository.branch_exists?('kittens')).to eq(false)
    end

    it 'returns false when using an invalid branch name' do
      expect(repository.branch_exists?('.bla')).to eq(false)
    end
  end

  describe '#local_branches' do
    let(:repository) { mutable_repository }

    before do
      create_remote_branch('joe', 'remote_branch', 'master')
      create_branch(repository, 'local_branch', 'master')
    end

    it 'returns the local branches' do
      expect(repository.local_branches.any? { |branch| branch.name == 'remote_branch' }).to eq(false)
      expect(repository.local_branches.any? { |branch| branch.name == 'local_branch' }).to eq(true)
    end
  end

  describe '#fetch_source_branch!' do
    let(:local_ref) { 'refs/merge-requests/1/head' }
    let(:repository) { mutable_repository }
    let(:source_repository) { repository }

    context 'when the branch exists' do
      context 'when the commit does not exist locally' do
        let(:source_branch) { 'new-branch-for-fetch-source-branch' }
        let(:source_path) { File.join(DEFAULT_STORAGE_DIR, source_repository.relative_path) }
        let(:source_rugged) { Rugged::Repository.new(source_path) }
        let(:new_oid) { new_commit_edit_old_file(source_rugged).oid }

        before do
          source_rugged.branches.create(source_branch, new_oid)
        end

        it 'writes the ref' do
          expect(repository.fetch_source_branch!(source_repository, source_branch, local_ref)).to eq(true)
          expect(repository.commit(local_ref).sha).to eq(new_oid)
        end
      end

      context 'when the commit exists locally' do
        let(:source_branch) { 'master' }
        let(:expected_oid) { SeedRepo::LastCommit::ID }

        it 'writes the ref' do
          # Sanity check: the commit should already exist
          expect(repository.commit(expected_oid)).not_to be_nil

          expect(repository.fetch_source_branch!(source_repository, source_branch, local_ref)).to eq(true)
          expect(repository.commit(local_ref).sha).to eq(expected_oid)
        end
      end
    end

    context 'when the branch does not exist' do
      let(:source_branch) { 'definitely-not-master' }

      it 'does not write the ref' do
        expect(repository.fetch_source_branch!(source_repository, source_branch, local_ref)).to eq(false)
        expect(repository.commit(local_ref)).to be_nil
      end
    end
  end

  describe '#rm_branch' do
    let(:repository) { mutable_repository }
    let(:branch_name) { "to-be-deleted-soon" }

    before do
      # TODO: project.add_developer(user)
      create_branch(repository, branch_name)
    end

    it "removes the branch from the repo" do
      repository.rm_branch(branch_name, user: user)

      expect(repository_rugged.branches[branch_name]).to be_nil
    end
  end

  describe '#write_ref' do
    let(:repository) { mutable_repository }

    context 'validations' do
      using RSpec::Parameterized::TableSyntax

      where(:ref_path, :ref) do
        'foo bar' | '123'
        'foobar'  | "12\x003"
      end

      with_them do
        it 'raises ArgumentError' do
          expect { repository.write_ref(ref_path, ref) }.to raise_error(ArgumentError)
        end
      end
    end

    it 'writes the HEAD' do
      repository.write_ref('HEAD', 'refs/heads/feature')

      expect(repository.commit('HEAD')).to eq(repository.commit('feature'))
      expect(repository.root_ref).to eq('feature')
    end

    it 'writes other refs' do
      repository.write_ref('refs/heads/feature', SeedRepo::Commit::ID)

      expect(repository.commit('feature').sha).to eq(SeedRepo::Commit::ID)
    end
  end

  describe '#write_config' do
    before do
      repository_rugged.config["gitlab.fullpath"] = repository_path
    end

    context 'is given a path' do
      it 'writes it to disk' do
        repository.write_config(full_path: "not-the/real-path.git")

        config = File.read(File.join(repository_path, "config"))

        expect(config).to include("[gitlab]")
        expect(config).to include("fullpath = not-the/real-path.git")
      end
    end

    context 'it is given an empty path' do
      it 'does not write it to disk' do
        repository.write_config(full_path: "")

        config = File.read(File.join(repository_path, "config"))

        expect(config).to include("[gitlab]")
        expect(config).to include("fullpath = #{repository_path}")
      end
    end
  end

  describe '#merge' do
    let(:repository) { mutable_repository }
    let(:source_sha) { '913c66a37b4a45b9769037c55c2d238bd0942d2e' }
    let(:target_branch) { 'test-merge-target-branch' }

    before do
      create_branch(repository, target_branch, '6d394385cf567f80a8fd85055db1ab4c5295806f')
    end

    it 'can perform a merge' do
      merge_commit_id = nil
      result = repository.merge(user, source_sha, target_branch, 'Test merge') do |commit_id|
        merge_commit_id = commit_id
      end

      expect(result.newrev).to eq(merge_commit_id)
      expect(result.repo_created).to eq(false)
      expect(result.branch_created).to eq(false)
    end

    it 'returns nil if there was a concurrent branch update' do
      concurrent_update_id = '33f3729a45c02fc67d00adb1b8bca394b0e761d9'
      result = repository.merge(user, source_sha, target_branch, 'Test merge') do
        # This ref update should make the merge fail
        repository.write_ref(Gitlab::Git::BRANCH_REF_PREFIX + target_branch, concurrent_update_id)
      end

      # This 'nil' signals that the merge was not applied
      expect(result).to be_nil

      # Our concurrent ref update should not have been undone
      expect(repository.find_branch(target_branch).target).to eq(concurrent_update_id)
    end
  end

  describe '#merge_to_ref' do
    let(:repository) { mutable_repository }
    let(:branch_head) { '6d394385cf567f80a8fd85055db1ab4c5295806f' }
    let(:source_sha) { 'cfe32cf61b73a0d5e9f13e774abde7ff789b1660' }
    let(:branch) { 'test-master' }
    let(:target_ref) { 'refs/merge-requests/999/merge' }

    before do
      create_branch(repository, branch, branch_head)
    end

    def fetch_target_ref
      repository.rugged.references[target_ref]
    end

    it 'changes target ref to a merge between source SHA and branch' do
      expect(fetch_target_ref).to be_nil

      merge_commit_id = repository.merge_to_ref(user, source_sha, branch, target_ref, 'foo')

      ref = fetch_target_ref

      expect(ref.target.oid).to eq(merge_commit_id)
    end

    it 'does not change the branch HEAD' do
      expect { repository.merge_to_ref(user, source_sha, branch, target_ref, 'foo') }
        .not_to change { repository.find_branch(branch).target }
        .from(branch_head)
    end
  end

  describe '#ff_merge' do
    let(:repository) { mutable_repository }
    let(:branch_head) { '6d394385cf567f80a8fd85055db1ab4c5295806f' }
    let(:source_sha) { 'cfe32cf61b73a0d5e9f13e774abde7ff789b1660' }
    let(:target_branch) { 'test-ff-target-branch' }

    before do
      create_branch(repository, target_branch, branch_head)
    end

    subject { repository.ff_merge(user, source_sha, target_branch) }

    it 'performs a ff_merge' do
      expect(subject.newrev).to eq(source_sha)
      expect(subject.repo_created).to be(false)
      expect(subject.branch_created).to be(false)

      expect(repository.commit(target_branch).id).to eq(source_sha)
    end

    context 'with a non-existing target branch' do
      subject { repository.ff_merge(user, source_sha, 'this-isnt-real') }

      it 'throws an ArgumentError' do
        expect { subject }.to raise_error(ArgumentError)
      end
    end

    context 'with a non-existing source commit' do
      let(:source_sha) { 'f001' }

      it 'throws an ArgumentError' do
        expect { subject }.to raise_error(ArgumentError)
      end
    end

    context 'when the source sha is not a descendant of the branch head' do
      let(:source_sha) { '1a0b36b3cdad1d2ee32457c102a8c0b7056fa863' }

      it "doesn't perform the ff_merge" do
        expect { subject }.to raise_error(Gitlab::Git::CommitError)

        expect(repository.commit(target_branch).id).to eq(branch_head)
      end
    end
  end

  describe 'remotes' do
    let(:repository) { mutable_repository }
    let(:remote_name) { 'my-remote' }
    let(:url) { 'http://my-repo.git' }

    describe '#add_remote' do
      let(:mirror_refmap) { '+refs/*:refs/*' }

      it 'added the remote' do
        begin
          repository_rugged.remotes.delete(remote_name)
        rescue Rugged::ConfigError # rubocop:disable Lint/HandleExceptions
        end

        repository.add_remote(remote_name, url, mirror_refmap: mirror_refmap)

        expect(repository_rugged.remotes[remote_name]).not_to be_nil
        expect(repository_rugged.config["remote.#{remote_name}.mirror"]).to eq('true')
        expect(repository_rugged.config["remote.#{remote_name}.prune"]).to eq('true')
        expect(repository_rugged.config["remote.#{remote_name}.fetch"]).to eq(mirror_refmap)
      end
    end

    describe '#remove_remote' do
      it 'removes the remote' do
        repository_rugged.remotes.create(remote_name, url)

        repository.remove_remote(remote_name)

        expect(repository_rugged.remotes[remote_name]).to be_nil
      end
    end
  end

  describe '#squash' do
    let(:repository) { mutable_repository }
    let(:squash_id) { '1' }
    let(:branch_name) { 'fix' }
    let(:start_sha) { '4b4918a572fa86f9771e5ba40fbd48e1eb03e2c6' }
    let(:end_sha) { '12d65c8dd2b2676fa3ac47d955accc085a37a9c1' }

    subject do
      opts = {
        branch: branch_name,
        start_sha: start_sha,
        end_sha: end_sha,
        author: user,
        message: 'Squash commit message'
      }

      repository.squash(user, squash_id, opts)
    end

    describe 'sparse checkout' do
      let(:expected_files) { %w[files files/js files/js/application.js] }

      it 'checks out only the files in the diff' do
        allow(repository).to receive(:with_worktree).and_wrap_original do |m, *args|
          m.call(*args) do
            worktree = args[0]
            files_pattern = File.join(worktree.path, '**', '*')
            expected = expected_files.map do |path|
              File.expand_path(path, worktree.path)
            end

            expect(Dir[files_pattern]).to eq(expected)
          end
        end

        subject
      end

      context 'when the diff contains a rename' do
        let(:end_sha) { new_commit_move_file(repository_rugged).oid }

        after do
          # Erase our commits so other tests get the original repo
          repository_rugged.references.update('refs/heads/master', SeedRepo::LastCommit::ID)
        end

        it 'does not include the renamed file in the sparse checkout' do
          allow(repository).to receive(:with_worktree).and_wrap_original do |m, *args|
            m.call(*args) do
              worktree = args[0]
              files_pattern = File.join(worktree.path, '**', '*')

              expect(Dir[files_pattern]).not_to include('CHANGELOG')
              expect(Dir[files_pattern]).not_to include('encoding/CHANGELOG')
            end
          end

          subject
        end
      end
    end

    describe 'with an ASCII-8BIT diff' do
      let(:diff) do
        <<~RAW_DIFF
          diff --git a/README.md b/README.md
          index faaf198..43c5edf 100644
          --- a/README.md
          +++ b/README.md
          @@ -1,4 +1,4 @@
          -testme
          +✓ testme
           ======

           Sample repo for testing gitlab features
        RAW_DIFF
      end

      it 'applies a ASCII-8BIT diff' do
        allow(repository).to receive(:run_git!).and_call_original
        allow(repository).to receive(:run_git!)
          .with(%W[diff --binary #{start_sha}...#{end_sha}])
          .and_return(diff.force_encoding('ASCII-8BIT'))

        expect(subject).to match(/\h{40}/)
      end
    end

    describe 'with trailing whitespace in an invalid patch' do
      let(:diff) do
        # rubocop:disable Layout/TrailingWhitespace
        <<~RAW_DIFF
          diff --git a/README.md b/README.md
          index faaf198..43c5edf 100644
          --- a/README.md
          +++ b/README.md
          @@ -1,4 +1,4 @@
          -testme
          +   
           ======   
           
           Sample repo for testing gitlab features
        RAW_DIFF
        # rubocop:enable Layout/TrailingWhitespace
      end

      it 'does not include whitespace warnings in the error' do
        allow(repository).to receive(:run_git!).and_call_original
        allow(repository).to receive(:run_git!)
          .with(%W[diff --binary #{start_sha}...#{end_sha}])
          .and_return(diff.force_encoding('ASCII-8BIT'))

        expect { subject }.to raise_error do |error|
          expect(error).to be_a(described_class::GitError)
          expect(error.message).not_to include('trailing whitespace')
        end
      end
    end
  end

  describe '#cleanup' do
    context 'when Rugged has been called' do
      it 'calls close on Rugged::Repository' do
        rugged = repository.rugged

        expect(rugged).to receive(:close).and_call_original

        repository.cleanup
      end
    end

    context 'when Rugged has not been called' do
      it 'does not call close on Rugged::Repository' do
        expect(repository).not_to receive(:rugged)

        repository.cleanup
      end
    end
  end

  describe '#rugged' do
    after do
      Thread.current[described_class::RUGGED_KEY] = nil
    end

    it 'stores reference in Thread.current' do
      Thread.current[described_class::RUGGED_KEY] = []

      2.times do
        rugged = repository.rugged

        expect(rugged).to be_a(Rugged::Repository)
        expect(Thread.current[described_class::RUGGED_KEY]).to eq([rugged])
      end
    end

    it 'does not store reference if Thread.current is not set up' do
      rugged = repository.rugged

      expect(rugged).to be_a(Rugged::Repository)
      expect(Thread.current[described_class::RUGGED_KEY]).to be_nil
    end
  end

  describe "#commit_patches" do
    let(:repository) { gitlab_git_from_gitaly(new_mutable_test_repo) }
    let(:testdata_dir) { File.join(File.dirname(__FILE__), '../../../../../internal/service/operations/testdata') }
    let(:patches) { File.foreach(File.join(testdata_dir, patch_file_name)) }

    def apply_patches(branch_name)
      repository.commit_patches(branch_name, patches)
    end

    context 'when the patch applies' do
      let(:patch_file_name) { '0001-A-commit-from-a-patch.patch' }

      it 'creates a new rev with the patch' do
        new_rev = apply_patches(repository.root_ref)
        commit = repository.commit(new_rev)

        expect(new_rev).not_to be_nil
        expect(commit.message).to eq("A commit from a patch\n")

        # Ensure worktree cleanup occurs
        result, status = repository.send(:run_git, %w[worktree list --porcelain])
        expect(status).to eq(0)
        expect(result).to eq("worktree #{repository_path}\nbare\n\n")
      end
    end

    context 'when the patch does not apply' do
      let(:patch_file_name) { '0001-This-does-not-apply-to-the-feature-branch.patch' }

      it 'raises a PatchError' do
        expect { apply_patches('feature') }.to raise_error Gitlab::Git::PatchError
      end
    end
  end

  describe '#update_submodule' do
    let(:new_oid) { 'db97db76ecd478eb361f439807438f82d97b29a5' }
    let(:repository) { gitlab_git_from_gitaly(new_mutable_test_repo) }
    let(:submodule) { 'gitlab-grack' }
    let(:head_commit) { repository.commit(branch) }
    let!(:head_submodule_reference) { repository.blob_at(head_commit.id, submodule).id }
    let(:committer) { repository.user_to_committer(user) }
    let(:message) { 'Update submodule' }
    let(:branch) { 'master' }

    subject do
      repository.update_submodule(submodule,
                                  new_oid,
                                  branch,
                                  committer,
                                  message)
    end

    it 'updates the submodule oid' do
      blob = repository.blob_at(subject, submodule)

      expect(blob.id).not_to eq head_submodule_reference
      expect(blob.id).to eq new_oid
    end
  end

  def create_remote_branch(remote_name, branch_name, source_branch_name)
    source_branch = repository.branches.find { |branch| branch.name == source_branch_name }
    repository_rugged.references.create("refs/remotes/#{remote_name}/#{branch_name}", source_branch.dereferenced_target.sha)
  end

  # Build the options hash that's passed to Rugged::Commit#create
  def commit_options(repo, index, message)
    options = {}
    options[:tree] = index.write_tree(repo)
    options[:author] = {
      email: "test@example.com",
      name: "Test Author",
      time: Time.gm(2014, "mar", 3, 20, 15, 1)
    }
    options[:committer] = {
      email: "test@example.com",
      name: "Test Author",
      time: Time.gm(2014, "mar", 3, 20, 15, 1)
    }
    options[:message] ||= message
    options[:parents] = repo.empty? ? [] : [repo.head.target].compact
    options[:update_ref] = "HEAD"

    options
  end

  # Writes a new commit to the repo and returns a Rugged::Commit.  Replaces the
  # contents of CHANGELOG with a single new line of text.
  def new_commit_edit_old_file(repo)
    oid = repo.write("I replaced the changelog with this text", :blob)
    index = repo.index
    index.read_tree(repo.head.target.tree)
    index.add(path: "CHANGELOG", oid: oid, mode: 0o100644)

    options = commit_options(
      repo,
      index,
      "Edit CHANGELOG in its original location"
    )

    sha = Rugged::Commit.create(repo, options)
    repo.lookup(sha)
  end

  # Writes a new commit to the repo and returns a Rugged::Commit.  Moves the
  # CHANGELOG file to the encoding/ directory.
  def new_commit_move_file(repo)
    blob_oid = repo.head.target.tree.detect { |i| i[:name] == "CHANGELOG" }[:oid]
    file_content = repo.lookup(blob_oid).content
    oid = repo.write(file_content, :blob)
    index = repo.index
    index.read_tree(repo.head.target.tree)
    index.add(path: "encoding/CHANGELOG", oid: oid, mode: 0o100644)
    index.remove("CHANGELOG")

    options = commit_options(repo, index, "Move CHANGELOG to encoding/")

    sha = Rugged::Commit.create(repo, options)
    repo.lookup(sha)
  end

  def create_branch(repository, branch_name, start_point = 'HEAD')
    repository.rugged.branches.create(branch_name, start_point)
  end
end
