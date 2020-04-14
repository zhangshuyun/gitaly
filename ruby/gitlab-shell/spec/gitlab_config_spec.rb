require_relative 'spec_helper'
require_relative '../lib/gitlab_config'

describe GitlabConfig do
  let(:config) { GitlabConfig.new }
  let(:config_data) { {} }

  before { expect(YAML).to receive(:load_file).and_return(config_data) }

  describe '#gitlab_url' do
    let(:url) { 'http://test.com' }

    subject { config.gitlab_url }

    before { config_data['gitlab_url'] = url }

    it { is_expected.not_to be_empty }
    it { is_expected.to eq(url) }

    context 'remove trailing slashes' do
      before { config_data['gitlab_url'] = url + '//' }

      it { is_expected.to eq(url) }
    end
  end

  describe '#secret_file' do
    subject { config.secret_file }

    it 'returns ".gitlab_shell_secret" by default' do
      is_expected.to eq(File.join(File.expand_path('..', __dir__),'.gitlab_shell_secret'))
    end
  end

  describe '#fetch_from_legacy_config' do
    let(:key) { 'yaml_key' }

    where(:yaml_value, :default, :expected_value) do
      [
        ['a', 'b', 'a'],
        [nil, 'b', 'b'],
        ['a', nil, 'a'],
        [nil, {}, {}]
      ]
    end

    with_them do
      it 'returns the correct value' do
        config_data[key] = yaml_value

        expect(config.fetch_from_legacy_config(key, default)).to eq(expected_value)
      end
    end
  end
end
