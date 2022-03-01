require 'open3'

module Gitlab
  module Git
    module Popen
      FAST_GIT_PROCESS_TIMEOUT = 15.seconds

      def popen(cmd, path, vars = {}, include_stderr: true, lazy_block: nil)
        raise "System commands must be given as an array of strings" unless cmd.is_a?(Array)

        path ||= Dir.pwd
        vars['PWD'] = path
        options = { chdir: path }

        cmd_output = ""
        cmd_status = 0
        Open3.popen3(vars, *cmd, options) do |stdin, stdout, stderr, wait_thr|
          stdout.set_encoding(Encoding::ASCII_8BIT)
          stderr.set_encoding(Encoding::ASCII_8BIT)

          # stderr and stdout pipes can block if stderr/stdout aren't drained: https://bugs.ruby-lang.org/issues/9082
          # Mimic what Ruby does with capture3: https://github.com/ruby/ruby/blob/1ec544695fa02d714180ef9c34e755027b6a2103/lib/open3.rb#L257-L273
          err_reader = Thread.new { stderr.read }

          begin
            yield(stdin) if block_given?
            stdin.close

            if lazy_block
              cmd_output = lazy_block.call(stdout.lazy)
              cmd_status = 0
              break
            else
              cmd_output << stdout.read
            end

            cmd_output << err_reader.value if include_stderr
            cmd_status = wait_thr.value.exitstatus
          ensure
            # When Popen3.open3 returns, the stderr reader gets closed, which causes
            # an exception in the err_reader thread. Kill the thread before
            # returning from Popen3.open3.
            err_reader.kill
          end
        end

        [cmd_output, cmd_status]
      end
    end
  end
end
