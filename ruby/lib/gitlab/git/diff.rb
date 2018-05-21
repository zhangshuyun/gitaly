module Gitlab
  module Git
    # These are monkey patches on top of the vendored version of Diff.
		class Diff
      attr_reader :old_id, :new_id

			def init_from_rugged(rugged)
        if rugged.is_a?(Rugged::Patch)
          init_from_rugged_patch(rugged)
          d = rugged.delta
        else
          d = rugged
        end

        @new_path = encode!(d.new_file[:path])
        @old_path = encode!(d.old_file[:path])
        @a_mode = d.old_file[:mode].to_s(8)
        @b_mode = d.new_file[:mode].to_s(8)
        @new_file = d.added?
        @renamed_file = d.renamed?
        @deleted_file = d.deleted?
				@old_id = d.old_file[:oid] || BLANK_SHA
				@new_id = d.new_file[:oid] || BLANK_SHA
      end
		end
	end
end
