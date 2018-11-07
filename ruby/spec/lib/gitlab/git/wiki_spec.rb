require 'spec_helper'

describe Gitlab::Git::Wiki do
  include TestRepo

  let(:repository) { gitlab_git_from_gitaly(new_empty_test_repo) }
  let(:user) { project.owner }

  subject { described_class.new(repository) }

  shared_examples 'with limit option' do
    it 'returns all the pages' do
      expect(pages.count).to eq(2)
      expect(pages.first.title).to eq 'page1'
      expect(pages.last.title).to eq 'page2'
    end

    it 'returns only one page' do
      expect(limited_pages.count).to eq(1)
      expect(limited_pages.first.title).to eq 'page1'
    end
  end

  describe '#pages' do
    let(:pages) { subject.pages }
    let(:limited_pages) { subject.pages(limit: 1) }

    before do
      create_page('page1', 'content')
      create_page('page2', 'content2')
    end

    after do
      destroy_page('page1')
      destroy_page('page2')
    end

    it_behaves_like 'with limit option'

    it 'returns formatted data' do
      expect(pages.first.formatted_data).to be_a(String)
    end

    it 'loads page raw_data' do
      pages.each do |page|
        expect(page.raw_data).not_to be_empty
      end
    end
  end

  describe '#list_pages' do
    let(:pages) { subject.list_pages }
    let(:limited_pages) { subject.list_pages(limit: 1) }

    before do
      create_page('page1', 'content')
      create_page('page2', 'content2')
    end

    after do
      destroy_page('page1')
      destroy_page('page2')
    end

    it_behaves_like 'with limit option'

    it 'does not load page raw_data' do
      pages.each do |page|
        expect(page.raw_data).to be_nil
      end
    end
  end

  describe '#page' do
    before do
      create_page('page1', 'content')
      create_page('foo/page1', 'content foo/page1')
    end

    after do
      destroy_page('page1')
      destroy_page('page1', 'foo')
    end

    it 'returns the right page' do
      expect(subject.page(title: 'page1', dir: '').url_path).to eq 'page1'
      expect(subject.page(title: 'page1', dir: 'foo').url_path).to eq 'foo/page1'
    end

    it 'returns formatted data' do
      expect(subject.page(title: 'page1', dir: '').formatted_data).to be_a(String)
    end
  end

  describe '#delete_page' do
    after do
      destroy_page('page1')
    end

    it 'only removes the page with the same path' do
      create_page('page1', 'content')
      create_page('*', 'content')

      subject.delete_page('*', commit_details('whatever'))

      expect(subject.pages.count).to eq 1
      expect(subject.pages.first.title).to eq 'page1'
    end
  end

  def create_page(name, content)
    subject.write_page(name, :markdown, content, commit_details(name))
  end

  def commit_details(name)
    Gitlab::Git::Wiki::CommitDetails.new(1, 'test-user', 'Test User', 'test@example.com', "created page #{name}")
  end

  def destroy_page(title, dir = '')
    page = subject.page(title: title, dir: dir)
    subject.delete_page(page.path, commit_details(title))
  end
end
