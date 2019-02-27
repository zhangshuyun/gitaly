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

    before do
      hook_names.each do |f|
        path = File.join(tmp_dir, f)
        File.write(path, script)
        FileUtils.chmod("u+x", path)
      end

      allow(Gitlab.config.git).to receive(:hooks_directory).and_return(tmp_dir)
      allow(Gitlab.config.gitlab_shell).to receive(:path).and_return('/foobar/gitlab-shell')
    end

    after do
      FileUtils.remove_entry(tmp_dir)
    end

    context 'when the hooks require environment variables' do
      let(:vars) do
        {
          'GL_ID' => 'user-123',
          'GL_USERNAME' => 'janedoe',
          'GL_REPOSITORY' => repo.gl_repository,
          'GL_PROTOCOL' => 'web',
          'PWD' => repo.path,
          'GIT_DIR' => repo.path,
          'GITLAB_SHELL_DIR' => '/foobar/gitlab-shell'
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
                                          .trigger(vars['GL_ID'], vars['GL_USERNAME'], '0' * 40, 'a' * 40, 'master')

          expect(trigger_result.first).to be(true), "#{hook} failed:  #{trigger_result.last}"
        end
      end
    end

    context 'when the hooks are successful' do
      let(:script) { "#!/bin/sh\nexit 0\n" }

      it 'returns true' do
        hook_names.each do |hook|
          trigger_result = described_class.new(hook, repo)
                                          .trigger('user-456', 'admin', '0' * 40, 'a' * 40, 'master')

          expect(trigger_result.first).to be(true)
        end
      end
    end

    context 'when the hooks fail' do
      let(:script) { "#!/bin/sh\nexit 1\n" }

      it 'returns false' do
        hook_names.each do |hook|
          trigger_result = described_class.new(hook, repo)
                                          .trigger('user-1', 'admin', '0' * 40, 'a' * 40, 'master')

          expect(trigger_result.first).to be(false)
        end
      end
    end

    describe 'sanitizing hook output' do
      let(:trigger_hook) do
        lambda do |hook|
          described_class.new(hook, repo).trigger('user-456', 'admin', '0' * 40, 'a' * 40, 'master')
        end
      end

      context 'when the hooks are successful' do
        let(:script) { stdout_stderr_script(exit_code: 0) }

        it 'does not return a message' do
          hook_names.each do |hook|
            silence_error_log

            trigger_result = trigger_hook[hook]

            expect(trigger_result.last).to be_nil
          end
        end

        it 'logs all stderr to the error log' do
          hook_names.each do |hook|
            error_message = format(
              Gitlab::Git::Hook::ERROR_LOG_FORMAT,
              hook,
              repo.relative_path,
              'GitLab: prefixed msg to STDERR'
            )

            expect(Gitlab::GitLogger).to receive(:error).with(error_message)

            trigger_hook[hook]
          end
        end
      end

      context 'when the hooks fail' do
        let(:script) { stdout_stderr_script(exit_code: 1) }

        it 'only returns messages that were marked as safe' do
          hook_names.each do |hook|
            silence_error_log

            trigger_result = trigger_hook[hook]

            expect(trigger_result.last).to eq(
              "prefixed msg to STDOUT\na second safe message with no whitespace separation\nprefixed msg to STDERR"
            )
          end
        end

        it 'logs all stderr to the error log' do
          hook_names.each do |hook|
            error_message = format(
              Gitlab::Git::Hook::ERROR_LOG_FORMAT,
              hook,
              repo.relative_path,
              'GitLab: prefixed msg to STDERR'
            )

            expect(Gitlab::GitLogger).to receive(:error).with(error_message)

            trigger_hook[hook]
          end
        end
      end
    end

    private

    def stdout_stderr_script(exit_code:)
      <<-SCRIPT
        #!/bin/sh
        echo "msg to STDOUT";
        1>&2 echo "#{Gitlab::Git::Hook::SAFE_MESSAGE_PREFIX} prefixed msg to STDERR";
        echo "#{Gitlab::Git::Hook::SAFE_MESSAGE_PREFIX} prefixed msg to STDOUT\nwith a second line";
        echo "#{Gitlab::Git::Hook::SAFE_MESSAGE_PREFIX}a second safe message with no whitespace separation";
        echo "not prefixed by, but containing, #{Gitlab::Git::Hook::SAFE_MESSAGE_PREFIX}";
        exit #{exit_code}
      SCRIPT
    end

    # Call before tests of scripts that write to stderr, when stderr is
    # not a subject of a test. This prevents the error from appearing in
    # rspec's output when rspec is running
    def silence_error_log
      expect(Gitlab::GitLogger).to receive(:error)
    end
  end
end
