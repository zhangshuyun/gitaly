module GitalyServer
  class DiffService < Gitaly::DiffService::Service
    include Utils

    def commit_patch(request, call)
      bridge_exceptions do
        repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
        commit = Gitlab::Git::Commit.find(repo, request.revision)

        Enumerator.new do |y|
          io = StringIO.new(commit.to_diff)
          while chunk = io.read(Gitlab.config.git.write_buffer_size)
            y.yield Gitaly::CommitPatchResponse.new(data: chunk)
          end
        end
      end
    end

    def commit_diff(request, call)
      bridge_exceptions do
        repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)

        options = {
          :ignore_whitespace_change => request.ignore_whitespace_change,
          :limits => request.enforce_limits,
          :expanded => !(request.enforce_limits || request.collapse_diffs),
          :max_lines => request.max_lines,
          :max_files => request.max_files,
          :max_bytes => request.max_bytes,
          :safe_max_lines => request.safe_max_lines,
          :safe_max_files => request.safe_max_files,
          :safe_max_bytes => request.safe_max_bytes,
        }

        Enumerator.new do |y|
          begin
            diffs = repo.diff(request.left_commit_id, request.right_commit_id, options, *request.paths.to_a)
            diffs.each do |diff|
              response = Gitaly::CommitDiffResponse.new(
                :from_path => diff.old_path.b,
                :to_path => diff.new_path.b,
                :from_id => diff.old_id,
                :to_id => diff.new_id,
                :old_mode => diff.a_mode.to_i(base=8),
                :new_mode => diff.b_mode.to_i(base=8),
                :binary => diff.has_binary_notice?,
                :overflow_marker => diff.too_large?,
                :collapsed => diff.collapsed?
              )
              io = StringIO.new(diff.diff)

              chunk = io.read(Gitlab.config.git.write_buffer_size)
              chunk = strip_diff_headers(chunk)
              response.raw_patch_data = chunk.b
              y.yield response
              while chunk = io.read(Gitlab.config.git.write_buffer_size)
                response.raw_patch_data = chunk.b
                y.yield response
              end
              response.raw_patch_data = ""
              response.end_of_patch = true
              y.yield response
            end
            if diffs.overflow?
              y.yield Gitaly::CommitDiffResponse.new(overflow_marker: true, end_of_patch: true)
            end
          rescue Rugged::ReferenceError => e
            raise GRPC::Unavailable.new(e.message)
          end
        end
      end
    end

    private

    # Stolen from Gitlab::Git::Diff since that is private

    # Strip out the information at the beginning of the patch's text to match
    # Grit's output
    def strip_diff_headers(diff_text)
      # Delete everything up to the first line that starts with '---' or
      # 'Binary'
      diff_text.sub!(/\A.*?^(@@ )/m, '\1')

      if diff_text.start_with?('@@ ')
        diff_text
      else
        # If the diff_text did not contain a line starting with '---' or
        # 'Binary', return the empty string. No idea why; we are just
        # preserving behavior from before the refactor.
        ''
      end
    end
  end
end
