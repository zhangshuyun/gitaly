# frozen_string_literal: true

module Gitlab
  module Git
    # Parses the output of a `git push --porcelain` command
    class PushResults
      attr_reader :all

      def initialize(raw_output)
        # If --porcelain is used, then each line of the output is of the form:
        #     <flag> \t <from>:<to> \t <summary> (<reason>)
        #
        # See https://git-scm.com/docs/git-push#_output
        @all = raw_output.each_line.map do |line|
          fields = line.split("\t")

          # Sanity check for porcelain output
          next unless fields.size >= 3

          # The fast-forward flag is a space, but there's also a space before
          # the tab delimiter, so we end up with a String of two spaces. Just
          # take the first character.
          flag = fields.shift.slice(0)

          next unless Result.valid_flag?(flag)

          from, to = fields.shift.split(':').map(&:strip)
          summary = fields.shift.strip

          Result.new(flag, from, to, summary)
        end.compact
      end

      # Returns an Array of branch names that were not rejected nor up-to-date
      def accepted_branches
        all.select(&:accepted?).collect(&:branch_name)
      end

      # Returns an Array of branch names that were rejected
      def rejected_branches
        all.select(&:rejected?).collect(&:branch_name)
      end

      Result = Struct.new(:flag_char, :from, :to, :summary) do
        # A single character indicating the status of the ref
        FLAGS = {
          ' ' => :fast_forward, # (space) for a successfully pushed fast-forward;
          '+' => :forced,       # +       for a successful forced update;
          '-' => :deleted,      # -       for a successfully deleted ref;
          '*' => :new,          # *       for a successfully pushed new ref;
          '!' => :rejected,     # !       for a ref that was rejected or failed to push; and
          '=' => :up_to_date    # =       for a ref that was up to date and did not need pushing.
        }.freeze

        def self.valid_flag?(flag)
          FLAGS.keys.include?(flag)
        end

        def flag
          FLAGS[flag_char]
        end

        def rejected?
          flag == :rejected
        end

        def accepted?
          !rejected? && flag != :up_to_date
        end

        def branch_name
          to.delete_prefix('refs/heads/')
        end
      end
    end
  end
end
