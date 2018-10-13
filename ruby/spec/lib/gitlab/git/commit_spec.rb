require "spec_helper"

describe Gitlab::Git::Commit do
  include TestRepo

  let(:repository) { gitlab_git_from_gitaly(git_test_repo_read_only) }
  let(:rugged_repo) do
    Rugged::Repository.new(GIT_TEST_REPO_PATH)
  end
  let(:commit) { described_class.find(repository, SeedRepo::Commit::ID) }
  let(:rugged_commit) { rugged_repo.lookup(SeedRepo::Commit::ID) }

  describe "Commit info" do
    before do
      @committer = {
        email: 'mike@smith.com',
        name: "Mike Smith",
        time: Time.now
      }

      @author = {
        email: 'john@smith.com',
        name: "John Smith",
        time: Time.now
      }

      @parents = [rugged_repo.head.target]
      @gitlab_parents = @parents.map { |c| described_class.find(repository, c.oid) }
      @tree = @parents.first.tree

      sha = Rugged::Commit.create(
        rugged_repo,
        author: @author,
        committer: @committer,
        tree: @tree,
        parents: @parents,
        message: "Refactoring specs",
        update_ref: "HEAD"
      )

      @raw_commit = rugged_repo.lookup(sha)
      @commit = described_class.find(repository, sha)
    end

    it { expect(@commit.short_id).to eq(@raw_commit.oid[0..10]) }
    it { expect(@commit.id).to eq(@raw_commit.oid) }
    it { expect(@commit.sha).to eq(@raw_commit.oid) }
    it { expect(@commit.safe_message).to eq(@raw_commit.message) }
    it { expect(@commit.created_at).to eq(@raw_commit.author[:time]) }
    it { expect(@commit.date).to eq(@raw_commit.committer[:time]) }
    it { expect(@commit.author_email).to eq(@author[:email]) }
    it { expect(@commit.author_name).to eq(@author[:name]) }
    it { expect(@commit.committer_name).to eq(@committer[:name]) }
    it { expect(@commit.committer_email).to eq(@committer[:email]) }
    it { expect(@commit.different_committer?).to be_truthy }
    it { expect(@commit.parents).to eq(@gitlab_parents) }
    it { expect(@commit.parent_id).to eq(@parents.first.oid) }
    it { expect(@commit.no_commit_message).to eq("--no commit message") }

    after do
      # Erase the new commit so other tests get the original repo
      rugged_repo.references.update("refs/heads/master", SeedRepo::LastCommit::ID)
    end
  end

  describe "Commit info from gitaly commit" do
    let(:subject) { "My commit".force_encoding('ASCII-8BIT') }
    let(:body) { subject + "My body".force_encoding('ASCII-8BIT') }
    let(:body_size) { body.length }
    let(:gitaly_commit) { build(:gitaly_commit, subject: subject, body: body, body_size: body_size) }
    let(:id) { gitaly_commit.id }
    let(:committer) { gitaly_commit.committer }
    let(:author) { gitaly_commit.author }
    let(:commit) { described_class.new(repository, gitaly_commit) }

    it { expect(commit.short_id).to eq(id[0..10]) }
    it { expect(commit.id).to eq(id) }
    it { expect(commit.sha).to eq(id) }
    it { expect(commit.safe_message).to eq(body) }
    it { expect(commit.created_at).to eq(Time.at(committer.date.seconds)) }
    it { expect(commit.author_email).to eq(author.email) }
    it { expect(commit.author_name).to eq(author.name) }
    it { expect(commit.committer_name).to eq(committer.name) }
    it { expect(commit.committer_email).to eq(committer.email) }
    it { expect(commit.parent_ids).to eq(gitaly_commit.parent_ids) }

    context 'body_size != body.size' do
      let(:body) { "".force_encoding('ASCII-8BIT') }

      context 'zero body_size' do
        it { expect(commit.safe_message).to eq(subject) }
      end
    end
  end

  context 'Class methods' do
    describe '.find' do
      it "returns an array of parent ids" do
        expect(described_class.find(repository, SeedRepo::Commit::ID).parent_ids).to be_an(Array)
      end

      it "should return valid commit for tag" do
        expect(described_class.find(repository, 'v1.0.0').id).to eq('6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9')
      end

      it "should return nil for non-commit ids" do
        blob = Gitlab::Git::Blob.find(repository, SeedRepo::Commit::ID, "files/ruby/popen.rb")
        expect(described_class.find(repository, blob.id)).to be_nil
      end

      it "should return nil for parent of non-commit object" do
        blob = Gitlab::Git::Blob.find(repository, SeedRepo::Commit::ID, "files/ruby/popen.rb")
        expect(described_class.find(repository, "#{blob.id}^")).to be_nil
      end

      it "should return nil for nonexisting ids" do
        expect(described_class.find(repository, "+123_4532530XYZ")).to be_nil
      end

      context 'with broken repo' do
        let(:repository) { gitlab_git_from_gitaly(new_broken_test_repo) }

        it 'returns nil' do
          expect(described_class.find(repository, SeedRepo::Commit::ID)).to be_nil
        end
      end
    end

    describe '.shas_with_signatures' do
      let(:signed_shas) { %w[5937ac0a7beb003549fc5fd26fc247adbce4a52e 570e7b2abdd848b95f2f578043fc23bd6f6fd24d] }
      let(:unsigned_shas) { %w[19e2e9b4ef76b422ce1154af39a91323ccc57434 c642fe9b8b9f28f9225d7ea953fe14e74748d53b] }
      let(:first_signed_shas) { %w[5937ac0a7beb003549fc5fd26fc247adbce4a52e c642fe9b8b9f28f9225d7ea953fe14e74748d53b] }

      it 'has 2 signed shas' do
        ret = described_class.shas_with_signatures(repository, signed_shas)
        expect(ret).to eq(signed_shas)
      end

      it 'has 0 signed shas' do
        ret = described_class.shas_with_signatures(repository, unsigned_shas)
        expect(ret).to eq([])
      end

      it 'has 1 signed sha' do
        ret = described_class.shas_with_signatures(repository, first_signed_shas)
        expect(ret).to contain_exactly(first_signed_shas.first)
      end
    end
  end

  describe 'move this test to gitaly-ruby' do
    describe '#init_from_rugged' do
      let(:gitlab_commit) { described_class.new(repository, rugged_commit) }
      subject { gitlab_commit }

      describe '#id' do
        subject { super().id }
        it { is_expected.to eq(SeedRepo::Commit::ID) }
      end
    end
  end

  describe '#init_from_hash' do
    let(:commit) { described_class.new(repository, sample_commit_hash) }
    subject { commit }

    describe '#id' do
      subject { super().id }
      it { is_expected.to eq(sample_commit_hash[:id])}
    end

    describe '#message' do
      subject { super().message }
      it { is_expected.to eq(sample_commit_hash[:message])}
    end
  end

  shared_examples '#stats' do
    subject { commit.stats }

    describe '#additions' do
      subject { super().additions }
      it { is_expected.to eq(11) }
    end

    describe '#deletions' do
      subject { super().deletions }
      it { is_expected.to eq(6) }
    end

    describe '#total' do
      subject { super().total }
      it { is_expected.to eq(17) }
    end
  end

  describe '#stats with gitaly on' do
    it_should_behave_like '#stats'
  end

  describe '#stats with gitaly disabled', :skip_gitaly_mock do
    it_should_behave_like '#stats'
  end

  describe '#has_zero_stats?' do
    it { expect(commit.has_zero_stats?).to eq(false) }
  end

  describe '#to_hash' do
    let(:hash) { commit.to_hash }
    subject { hash }

    it { is_expected.to be_kind_of Hash }

    describe '#keys' do
      subject { super().keys.sort }
      it { is_expected.to match(sample_commit_hash.keys.sort) }
    end
  end

  def sample_commit_hash
    {
      author_email: "dmitriy.zaporozhets@gmail.com",
      author_name: "Dmitriy Zaporozhets",
      authored_date: "2012-02-27 20:51:12 +0200",
      committed_date: "2012-02-27 20:51:12 +0200",
      committer_email: "dmitriy.zaporozhets@gmail.com",
      committer_name: "Dmitriy Zaporozhets",
      id: SeedRepo::Commit::ID,
      message: "tree css fixes",
      parent_ids: ["874797c3a73b60d2187ed6e2fcabd289ff75171e"]
    }
  end
end
