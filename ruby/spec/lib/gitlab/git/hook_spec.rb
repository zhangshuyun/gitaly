require 'spec_helper'

describe Gitlab::Git::Hook do
  include TestRepo

  describe '.directory' do
    it 'does not raise an KeyError' do
      expect { described_class.directory }.not_to raise_error
    end
  end

  describe '#trigger' do
    let(:tmp_dir) { Dir.mktmpdir }
    let(:hook_names) { %w[pre-receive post-receive update] }
    let(:repo) { gitlab_git_from_gitaly(test_repo_read_only) }
    let(:push_options) do
      Gitlab::Git::PushOptions.new(['ci.skip'])
    end

    def trigger_with_stub_data(hook, push_options)
      hook.trigger('user-1', 'admin', '0' * 40, 'a' * 40, 'master', push_options: push_options)
    end

    before do
      hook_names.each do |f|
        path = File.join(tmp_dir, f)
        File.write(path, script)
        FileUtils.chmod("u+x", path)
      end

      allow(Gitlab.config.git).to receive(:hooks_directory).and_return(tmp_dir)
    end

    after do
      FileUtils.remove_entry(tmp_dir)
    end

    context 'when the hooks require environment variables' do
      let(:vars) do
        {
          'GITALY_HOOKS_PAYLOAD' => Base64.strict_encode64({
            repository: repo.gitaly_repository.to_json,
            binary_directory: Gitlab.config.gitaly.bin_dir,
            git_path: Gitlab.config.git.bin_path,
            internal_socket: Gitlab.config.gitaly.internal_socket,
            internal_socket_token: nil,
            receive_hooks_payload: {
              userid: 'user-123',
              username: 'janedoe',
              protocol: 'web'
            }
          }.to_json),
          'PWD' => repo.path,
          'GIT_DIR' => repo.path
        }
      end

      let(:script) do
        [
          "#!/bin/sh",
          vars.map do |key, value|
            <<-SCRIPT
              if [ x$#{key} != x#{value} ]; then
                echo "unexpected value: #{key}=$#{key}"
                exit 1
               fi
            SCRIPT
          end.join,
          "exit 0"
        ].join("\n")
      end

      it 'returns true' do
        hook_names.each do |hook|
          trigger_result = described_class.new(hook, repo)
                                          .trigger('user-123', 'janedoe', '0' * 40, 'a' * 40, 'master', push_options: push_options)

          expect(trigger_result.first).to be(true), "#{hook} failed:  #{trigger_result.last}"
        end
      end
    end

    context 'when the hooks are successful' do
      let(:script) { "#!/bin/sh\nexit 0\n" }

      it 'returns true' do
        hook_names.each do |hook|
          trigger_result = described_class.new(hook, repo)
                                          .trigger('user-456', 'admin', '0' * 40, 'a' * 40, 'master', push_options: push_options)

          expect(trigger_result.first).to be(true)
        end
      end
    end

    context 'when the hooks fail' do
      let(:script) { "#!/bin/sh\nexit 1\n" }

      it 'returns false' do
        hook_names.each do |name|
          hook = described_class.new(name, repo)
          trigger_result = trigger_with_stub_data(hook, push_options)

          expect(trigger_result.first).to be(false)
        end
      end
    end

    context 'when push options are passed' do
      let(:script) do
        <<~HOOK
          #!/usr/bin/env ruby
          unless ENV['GIT_PUSH_OPTION_COUNT'] == '1' && ENV['GIT_PUSH_OPTION_0'] == 'ci.skip'
            abort 'missing GIT_PUSH_OPTION env vars'
          end
        HOOK
      end

      context 'for pre-receive and post-receive hooks' do
        let(:hooks) do
          %w[pre-receive post-receive].map { |name| described_class.new(name, repo) }
        end

        it 'sets the push options environment variables' do
          hooks.each do |hook|
            trigger_result = trigger_with_stub_data(hook, push_options)

            expect(trigger_result.first).to be(true)
          end
        end
      end

      context 'for update hook' do
        let(:hook) { described_class.new('update', repo) }

        it 'does not set the push options environment variables' do
          trigger_result = trigger_with_stub_data(hook, push_options)

          expect(trigger_result.first).to be(false)
        end
      end
    end
  end
end
