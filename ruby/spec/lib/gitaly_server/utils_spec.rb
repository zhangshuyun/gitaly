require 'spec_helper'

describe GitalyServer::Utils do
  let(:cls) do
    Class.new do
      include GitalyServer::Utils
    end
  end

  describe '.set_utf8!' do
    context 'valid encoding' do
      it 'returns a UTF-8 string' do
        str = 'écoles'

        expect(cls.new.set_utf8!(str.b)).to eq(str)
      end
    end

    context 'invalid encoding' do
      it 'returns a UTF-8 string' do
        str = "\xA9coles".b

        expect { cls.new.set_utf8!(str) }.to raise_error(ArgumentError)
      end
    end
  end

  describe '.gitaly_commit_from_rugged' do
    it 'truncates commit body if it exceeded a certain limit' do
      repo = Rugged::Repository.new(TEST_REPO_PATH)
      rugged_commit = repo.rev_parse('HEAD')
      full_message = "subject\n\n" + ("a" * 100 * 1024)
      limit = 10 * 1024

      allow_any_instance_of(Gitlab::Config::Git).to receive(:max_commit_or_tag_message_size).and_return(limit)
      allow(rugged_commit).to receive(:message).and_return(full_message)

      gitaly_commit = cls.new.gitaly_commit_from_rugged(rugged_commit)

      expect(gitaly_commit.body).to eq(full_message[0, limit])
      expect(gitaly_commit.body_size).to eq(full_message.bytesize)
    end
  end
end
