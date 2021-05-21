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

  describe '#with_repo_branch_commit' do
    let(:start_repository) { Gitlab::Git::RemoteRepository.new(source_repository) }
    let(:start_commit) { source_repository.commit }

    context 'when start_repository is empty' do
      let(:source_repository) { gitlab_git_from_gitaly(new_empty_test_repo) }

      before do
        expect(start_repository).not_to receive(:commit_id)
        expect(repository).not_to receive(:fetch_sha)
      end

      it 'yields nil' do
        expect do |block|
          repository.with_repo_branch_commit(start_repository, 'master', &block)
        end.to yield_with_args(nil)
      end
    end

    context 'when start_repository is the same repository' do
      let(:source_repository) { repository }

      before do
        expect(start_repository).not_to receive(:commit_id)
        expect(repository).not_to receive(:fetch_sha)
      end

      it 'yields the commit for the SHA' do
        expect do |block|
          repository.with_repo_branch_commit(start_repository, start_commit.sha, &block)
        end.to yield_with_args(start_commit)
      end

      it 'yields the commit for the branch' do
        expect do |block|
          repository.with_repo_branch_commit(start_repository, 'master', &block)
        end.to yield_with_args(start_commit)
      end
    end

    context 'when start_repository is different' do
      let(:source_repository) { gitlab_git_from_gitaly(test_repo_read_only) }

      context 'when start commit already exists' do
        let(:start_commit) { repository.commit }

        before do
          expect(start_repository).to receive(:commit_id).and_return(start_commit.sha)
          expect(repository).not_to receive(:fetch_sha)
        end

        it 'yields the commit for the SHA' do
          expect do |block|
            repository.with_repo_branch_commit(start_repository, start_commit.sha, &block)
          end.to yield_with_args(start_commit)
        end

        it 'yields the commit for the branch' do
          expect do |block|
            repository.with_repo_branch_commit(start_repository, 'master', &block)
          end.to yield_with_args(start_commit)
        end
      end

      context 'when start commit does not exist' do
        before do
          expect(start_repository).to receive(:commit_id).and_return(start_commit.sha)
          expect(repository).to receive(:fetch_sha).with(start_repository, start_commit.sha)
        end

        it 'yields the fetched commit for the SHA' do
          expect do |block|
            repository.with_repo_branch_commit(start_repository, start_commit.sha, &block)
          end.to yield_with_args(nil) # since fetch_sha is mocked
        end

        it 'yields the fetched commit for the branch' do
          expect do |block|
            repository.with_repo_branch_commit(start_repository, 'master', &block)
          end.to yield_with_args(nil) # since fetch_sha is mocked
        end
      end
    end
  end

  describe '#fetch_sha' do
    let(:source_repository) { Gitlab::Git::RemoteRepository.new(repository) }
    let(:sha) { 'b971194ee2d047f24cb897b6fb0d7ae99c8dd0ca' }
    let(:git_args) { %W[fetch --no-tags ssh://gitaly/internal.git #{sha}] }

    before do
      expect(source_repository).to receive(:fetch_env)
        .with(git_config_options: ['uploadpack.allowAnySHA1InWant=true'])
        .and_return({})
    end

    it 'fetches the commit from the source repository' do
      expect(repository).to receive(:run_git)
        .with(git_args, env: {}, include_stderr: true)
        .and_return(['success', 0])

      expect(repository.fetch_sha(source_repository, sha)).to eq(sha)
    end

    it 'raises an error if the commit does not exist in the source repository' do
      expect(repository).to receive(:run_git)
        .with(git_args, env: {}, include_stderr: true)
        .and_return(['error', 1])

      expect do
        repository.fetch_sha(source_repository, sha)
      end.to raise_error(Gitlab::Git::CommandError, 'error')
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
  end

  describe '#rebase' do
    let(:repository) { mutable_repository }
    let(:rebase_id) { '2' }
    let(:branch_name) { 'rd-add-file-larger-than-1-mb' }
    let(:branch_sha) { 'c54ad072fabee9f7bf9b2c6c67089db97ebfbecd' }
    let(:remote_branch) { 'master' }

    subject do
      opts = {
        branch: branch_name,
        branch_sha: branch_sha,
        remote_repository: repository,
        remote_branch: remote_branch
      }

      repository.rebase(user, rebase_id, **opts)
    end

    describe 'sparse checkout' do
      let(:expected_files) { %w[files/images/emoji.png] }

      it 'lists files modified in source branch in sparse-checkout' do
        allow(repository).to receive(:with_worktree).and_wrap_original do |m, *args, **kwargs|
          m.call(*args, **kwargs) do
            worktree = args[0]
            sparse = repository.path + "/worktrees/#{worktree.name}/info/sparse-checkout"
            diff_files = IO.readlines(sparse, chomp: true)

            expect(diff_files).to eq(expected_files)
          end
        end

        subject
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
    let(:testdata_dir) { File.join(File.dirname(__FILE__), '../../../../../internal/gitaly/service/operations/testdata') }
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

  def create_branch(repository, branch_name, start_point = 'HEAD')
    repository.rugged.branches.create(branch_name, start_point)
  end
end
