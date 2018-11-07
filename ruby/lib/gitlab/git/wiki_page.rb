module Gitlab
  module Git
    class WikiPage
      attr_reader :url_path, :title, :format, :path, :version, :raw_data, :name, :historical

      def initialize(gollum_page, version, load_content: true)
        @gollum_page = gollum_page

        @url_path = gollum_page.url_path
        @title = gollum_page.title
        @format = gollum_page.format
        @path = gollum_page.path
        @raw_data = gollum_page.raw_data if load_content
        @name = gollum_page.name
        @historical = gollum_page.historical?

        @version = version
      end

      def formatted_data
        @raw_data && @gollum_page.formatted_data
      end

      def historical?
        @historical
      end

      def text_data
        return @text_data if defined?(@text_data)

        @text_data = @raw_data && Gitlab::EncodingHelper.encode!(@raw_data.dup)
      end
    end
  end
end
