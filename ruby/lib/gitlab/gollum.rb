module Gollum
  GIT_ADAPTER = "rugged".freeze
end
require "gollum-lib"

Gollum::Page.per_page = 20 # Magic number from Kaminari.config.default_per_page

module Gollum
  class Page
    def text_data(encoding = nil)
      data = if raw_data.respond_to?(:encoding)
               raw_data.force_encoding(encoding || Encoding::UTF_8)
             else
               raw_data
             end

      Gitlab::EncodingHelper.encode!(data)
    end
  end

  # Override BlobEntry.normalize_dir to remove the call to File.expand_path,
  # which also expands `~` and `~username` paths, and can raise exceptions for
  # invalid users.
  #
  # We don't need to worry about symlinks or Windows paths, we only need to
  # normalize the slashes in the path, and return an empty string for toplevel
  # paths.
  class BlobEntry
    def self.normalize_dir(dir)
      # Return empty string for nil and paths that point to the toplevel
      # ('.', '/', '..' etc.)
      return '' if !dir || dir =~ %r{\A[\./]*\z}

      # Normalize the path:
      # - Add exactly one leading slash
      # - Remove trailing slashes
      # - Remove repeated slashes
      dir.sub(%r{
        \A
        /*            # leading slashes
        (?<path>.*?)  # the actual path
        /*            # trailing slashes
        \z
      }x, '/\k<path>').gsub(%r{//+}, '/')
    end
  end
end
