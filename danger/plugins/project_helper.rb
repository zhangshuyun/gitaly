# frozen_string_literal: true

module Danger
  # Common helper functions for danger scripts
  class ProjectHelper < ::Danger::Plugin
    # First-match win, so be sure to put more specific regex at the top...
    CATEGORIES = {
      %r{\Adoc/.*(\.(md|png|gif|jpg))\z} => :docs,
      %r{\A(CONTRIBUTING|LICENSE|README|REVIEWING|STYLE)(\.md)?\z} => :docs,

      %r{.*} => [nil]
    }.freeze

    def changes_by_category
      helper.changes_by_category(CATEGORIES)
    end

    def project_name
      'gitaly'
    end
  end
end
