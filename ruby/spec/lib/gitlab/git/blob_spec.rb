require "spec_helper"

describe Gitlab::Git::Blob do
  include TestRepo

  let(:repository) { gitlab_git_from_gitaly(git_test_repo_read_only) }
  let(:rugged) do
    Rugged::Repository.new(GIT_TEST_REPO_PATH)
  end

  describe 'initialize' do
    let(:blob) { Gitlab::Git::Blob.new(name: 'test') }

    it 'handles nil data' do
      expect(blob.name).to eq('test')
      expect(blob.size).to eq(nil)
      expect(blob.loaded_size).to eq(nil)
    end
  end

  describe '.find' do
    context 'nil path' do
      let(:blob) { Gitlab::Git::Blob.find(repository, SeedRepo::Commit::ID, nil) }

      it { expect(blob).to eq(nil) }
    end

    context 'utf-8 branch' do
      let(:blob) { Gitlab::Git::Blob.find(repository, 'Ääh-test-utf-8', "files/ruby/popen.rb") }

      it { expect(blob.id).to eq(SeedRepo::RubyBlob::ID) }
    end

    context 'blank path' do
      let(:blob) { Gitlab::Git::Blob.find(repository, SeedRepo::Commit::ID, '') }

      it { expect(blob).to eq(nil) }
    end

    context 'file in subdir' do
      let(:blob) { Gitlab::Git::Blob.find(repository, SeedRepo::Commit::ID, "files/ruby/popen.rb") }

      it { expect(blob.id).to eq(SeedRepo::RubyBlob::ID) }
      it { expect(blob.name).to eq(SeedRepo::RubyBlob::NAME) }
      it { expect(blob.path).to eq("files/ruby/popen.rb") }
      it { expect(blob.commit_id).to eq(SeedRepo::Commit::ID) }
      it { expect(blob.data[0..10]).to eq(SeedRepo::RubyBlob::CONTENT[0..10]) }
      it { expect(blob.size).to eq(669) }
      it { expect(blob.mode).to eq("100644") }
    end

    context 'file in root' do
      let(:blob) { Gitlab::Git::Blob.find(repository, SeedRepo::Commit::ID, ".gitignore") }

      it { expect(blob.id).to eq("dfaa3f97ca337e20154a98ac9d0be76ddd1fcc82") }
      it { expect(blob.name).to eq(".gitignore") }
      it { expect(blob.path).to eq(".gitignore") }
      it { expect(blob.commit_id).to eq(SeedRepo::Commit::ID) }
      it { expect(blob.data[0..10]).to eq("*.rbc\n*.sas") }
      it { expect(blob.size).to eq(241) }
      it { expect(blob.mode).to eq("100644") }
      it { expect(blob).not_to be_binary }
    end

    context 'file in root with leading slash' do
      let(:blob) { Gitlab::Git::Blob.find(repository, SeedRepo::Commit::ID, "/.gitignore") }

      it { expect(blob.id).to eq("dfaa3f97ca337e20154a98ac9d0be76ddd1fcc82") }
      it { expect(blob.name).to eq(".gitignore") }
      it { expect(blob.path).to eq(".gitignore") }
      it { expect(blob.commit_id).to eq(SeedRepo::Commit::ID) }
      it { expect(blob.data[0..10]).to eq("*.rbc\n*.sas") }
      it { expect(blob.size).to eq(241) }
      it { expect(blob.mode).to eq("100644") }
    end

    context 'non-exist file' do
      let(:blob) { Gitlab::Git::Blob.find(repository, SeedRepo::Commit::ID, "missing.rb") }

      it { expect(blob).to be_nil }
    end

    context 'six submodule' do
      let(:blob) { Gitlab::Git::Blob.find(repository, SeedRepo::Commit::ID, 'six') }

      it { expect(blob.id).to eq('409f37c4f05865e4fb208c771485f211a22c4c2d') }
      it { expect(blob.data).to eq('') }

      it 'does not mark the blob as binary' do
        expect(blob).not_to be_binary
      end
    end

    context 'large file' do
      let(:blob) { Gitlab::Git::Blob.find(repository, SeedRepo::Commit::ID, 'files/images/6049019_460s.jpg') }
      let(:blob_size) { 111_803 }
      let(:stub_limit) { 1000 }

      before do
        stub_const('Gitlab::Git::Blob::MAX_DATA_DISPLAY_SIZE', stub_limit)
      end

      it { expect(blob.size).to eq(blob_size) }
      it { expect(blob.data.length).to eq(stub_limit) }

      it 'check that this test is sane' do
        # It only makes sense to test limiting if the blob is larger than the limit.
        expect(blob.size).to be > Gitlab::Git::Blob::MAX_DATA_DISPLAY_SIZE
      end

      it 'marks the blob as binary' do
        expect(Gitlab::Git::Blob).to receive(:new)
          .with(hash_including(binary: true))
          .and_call_original

        expect(blob).to be_binary
      end
    end
  end

  describe 'encoding' do
    context 'file with russian text' do
      let(:blob) { Gitlab::Git::Blob.find(repository, SeedRepo::Commit::ID, "encoding/russian.rb") }

      it { expect(blob.name).to eq("russian.rb") }
      it { expect(blob.data.lines.first).to eq("Хороший файл") }
      it { expect(blob.size).to eq(23) }
      it { expect(blob.truncated?).to be_falsey }
      # Run it twice since data is encoded after the first run
      it { expect(blob.truncated?).to be_falsey }
      it { expect(blob.mode).to eq("100755") }
    end

    context 'file with Chinese text' do
      let(:blob) { Gitlab::Git::Blob.find(repository, SeedRepo::Commit::ID, "encoding/テスト.txt") }

      it { expect(blob.name).to eq("テスト.txt") }
      it { expect(blob.data).to include("これはテスト") }
      it { expect(blob.size).to eq(340) }
      it { expect(blob.mode).to eq("100755") }
      it { expect(blob.truncated?).to be_falsey }
    end

    context 'file with ISO-8859 text' do
      let(:blob) { Gitlab::Git::Blob.find(repository, SeedRepo::LastCommit::ID, "encoding/iso8859.txt") }

      it { expect(blob.name).to eq("iso8859.txt") }
      it { expect(blob.loaded_size).to eq(4) }
      it { expect(blob.size).to eq(4) }
      it { expect(blob.mode).to eq("100644") }
      it { expect(blob.truncated?).to be_falsey }
    end
  end

  describe 'mode' do
    context 'file regular' do
      let(:blob) do
        Gitlab::Git::Blob.find(
          repository,
          'fa1b1e6c004a68b7d8763b86455da9e6b23e36d6',
          'files/ruby/regex.rb'
        )
      end

      it { expect(blob.name).to eq('regex.rb') }
      it { expect(blob.path).to eq('files/ruby/regex.rb') }
      it { expect(blob.size).to eq(1200) }
      it { expect(blob.mode).to eq("100644") }
    end

    context 'file binary' do
      let(:blob) do
        Gitlab::Git::Blob.find(
          repository,
          'fa1b1e6c004a68b7d8763b86455da9e6b23e36d6',
          'files/executables/ls'
        )
      end

      it { expect(blob.name).to eq('ls') }
      it { expect(blob.path).to eq('files/executables/ls') }
      it { expect(blob.size).to eq(110_080) }
      it { expect(blob.mode).to eq("100755") }
    end

    context 'file symlink to regular' do
      let(:blob) do
        Gitlab::Git::Blob.find(
          repository,
          'fa1b1e6c004a68b7d8763b86455da9e6b23e36d6',
          'files/links/ruby-style-guide.md'
        )
      end

      it { expect(blob.name).to eq('ruby-style-guide.md') }
      it { expect(blob.path).to eq('files/links/ruby-style-guide.md') }
      it { expect(blob.size).to eq(31) }
      it { expect(blob.mode).to eq("120000") }
    end

    context 'file symlink to binary' do
      let(:blob) do
        Gitlab::Git::Blob.find(
          repository,
          'fa1b1e6c004a68b7d8763b86455da9e6b23e36d6',
          'files/links/touch'
        )
      end

      it { expect(blob.name).to eq('touch') }
      it { expect(blob.path).to eq('files/links/touch') }
      it { expect(blob.size).to eq(20) }
      it { expect(blob.mode).to eq("120000") }
    end
  end
end
