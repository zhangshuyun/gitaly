module Gitlab
  module Git
    class Blob
      include Linguist::BlobHelper
      include Gitlab::EncodingHelper

      # This number is the maximum amount of data that we want to display to
      # the user. We load as much as we can for encoding detection
      # (Linguist) and LFS pointer parsing.
      MAX_DATA_DISPLAY_SIZE = 10.megabytes

      attr_accessor :size, :mode, :id, :commit_id, :loaded_size, :binary
      attr_writer :data, :name, :path

      class << self
        def find(repository, sha, path, limit: MAX_DATA_DISPLAY_SIZE)
          return unless path

          # Strip any leading / characters from the path
          path = path.sub(%r{\A/*}, '')

          rugged_commit = repository.lookup(sha)
          root_tree = rugged_commit.tree

          blob_entry = find_entry_by_path(repository, root_tree.oid, *path.split('/'))

          return nil unless blob_entry

          if blob_entry[:type] == :commit
            submodule_blob(blob_entry, path, sha)
          else
            blob = repository.lookup(blob_entry[:oid])

            if blob
              new(
                id: blob.oid,
                name: blob_entry[:name],
                size: blob.size,
                # Rugged::Blob#content is expensive; don't call it if we don't have to.
                data: limit.zero? ? '' : blob.content(limit),
                mode: blob_entry[:filemode].to_s(8),
                path: path,
                commit_id: sha,
                binary: blob.binary?
              )
            end
          end
        rescue Rugged::ReferenceError
          nil
        end

        def binary?(data)
          EncodingHelper.detect_libgit2_binary?(data)
        end

        private

        # Recursive search of blob id by path
        #
        # Ex.
        #   blog/            # oid: 1a
        #     app/           # oid: 2a
        #       models/      # oid: 3a
        #       file.rb      # oid: 4a
        #
        #
        # Blob.find_entry_by_path(repo, '1a', 'blog', 'app', 'file.rb') # => '4a'
        #
        def find_entry_by_path(repository, root_id, *path_parts)
          root_tree = repository.lookup(root_id)

          entry = root_tree.find do |entry|
            entry[:name] == path_parts[0]
          end

          return nil unless entry

          if path_parts.size > 1
            return nil unless entry[:type] == :tree

            path_parts.shift
            find_entry_by_path(repository, entry[:oid], *path_parts)
          else
            [:blob, :commit].include?(entry[:type]) ? entry : nil
          end
        end

        def submodule_blob(blob_entry, path, sha)
          new(
            id: blob_entry[:oid],
            name: blob_entry[:name],
            size: 0,
            data: '',
            path: path,
            commit_id: sha
          )
        end
      end

      def initialize(options)
        %w(id name path size data mode commit_id binary).each do |key|
          self.__send__("#{key}=", options[key.to_sym])
        end

        # Retain the actual size before it is encoded
        @loaded_size = @data.bytesize if @data
        @loaded_all_data = @loaded_size == size
      end

      def binary?
        @binary.nil? ? super : @binary == true
      end

      def data
        encode! @data
      end

      def name
        encode! @name
      end

      def path
        encode! @path
      end

      def truncated?
        size && (size > loaded_size)
      end
    end
  end
end
