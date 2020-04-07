require_relative 'spec_helper'
require_relative '../lib/gitlab_net'
require_relative '../lib/gitlab_access_status'

describe GitlabNet, vcr: true do
  using RSpec::Parameterized::TableSyntax

  let(:gitlab_net) { described_class.new }
  let(:changes) { ['0000000000000000000000000000000000000000 92d0970eefd7acb6d548878925ce2208cfe2d2ec refs/heads/branch4'] }
  let(:base_api_endpoint) { 'http://localhost:3000/api/v4' }
  let(:internal_api_endpoint) { 'http://localhost:3000/api/v4/internal' }
  let(:project) { 'gitlab-org/gitlab-test.git' }
  let(:key) { 'key-1' }
  let(:key2) { 'key-2' }
  let(:secret) { "0a3938d9d95d807e94d937af3a4fbbea\n" }

  before do
    $logger = double('logger').as_null_object
    allow(gitlab_net).to receive(:base_api_endpoint).and_return(base_api_endpoint)
    allow(gitlab_net).to receive(:secret_token).and_return(secret)
  end

  describe '#check' do
    it 'should return 200 code for gitlab check' do
      VCR.use_cassette("check-ok") do
        result = gitlab_net.check
        expect(result.code).to eq('200')
      end
    end

    it 'adds the secret_token to request' do
      VCR.use_cassette("check-ok") do
        expect_any_instance_of(Net::HTTP::Get).to receive(:set_form_data).with(hash_including(secret_token: secret))
        gitlab_net.check
      end
    end

    it "raises an exception if the connection fails" do
      allow_any_instance_of(Net::HTTP).to receive(:request).and_raise(StandardError)
      expect { gitlab_net.check }.to raise_error(GitlabNet::ApiUnreachableError)
    end
  end

  describe '#pre_receive' do
    let(:gl_repository) { "project-1" }
    let(:params) { { gl_repository: gl_repository } }

    subject { gitlab_net.pre_receive(gl_repository) }

    it 'sends the correct parameters and returns the request body parsed' do
      expect_any_instance_of(Net::HTTP::Post).to receive(:set_form_data)
        .with(hash_including(params))

      VCR.use_cassette("pre-receive") { subject }
    end

    it 'calls /internal/pre-receive' do
      VCR.use_cassette("pre-receive") do
        expect(subject['reference_counter_increased']).to be(true)
      end
    end

    it 'throws a NotFound error when pre-receive is not available' do
      VCR.use_cassette("pre-receive-not-found") do
        expect { subject }.to raise_error(GitlabNet::NotFound)
      end
    end
  end

  describe '#post_receive' do
    let(:gl_repository) { "project-1" }
    let(:changes) { "123456 789012 refs/heads/test\n654321 210987 refs/tags/tag" }
    let(:push_options) { ["ci-skip", "something unexpected"] }
    let(:params) do
      { gl_repository: gl_repository, identifier: key, changes: changes, :"push_options[]" => push_options }
    end
    let(:merge_request_urls) do
      [{
        "branch_name" => "test",
        "url" => "http://localhost:3000/gitlab-org/gitlab-test/merge_requests/7",
        "new_merge_request" => false
      }]
    end

    subject { gitlab_net.post_receive(gl_repository, key, changes, push_options) }

    it 'sends the correct parameters' do
      expect_any_instance_of(Net::HTTP::Post).to receive(:set_form_data).with(hash_including(params))


      VCR.use_cassette("post-receive") do
        subject
      end
    end

    it 'calls /internal/post-receive' do
      VCR.use_cassette("post-receive") do
        expect(subject['merge_request_urls']).to eq(merge_request_urls)
        expect(subject['broadcast_message']).to eq('Message')
        expect(subject['reference_counter_decreased']).to eq(true)
      end
    end

    it 'throws a NotFound error when post-receive is not available' do
      VCR.use_cassette("post-receive-not-found") do
        expect { subject }.to raise_error(GitlabNet::NotFound)
      end
    end
  end

  describe '#check_access' do
    context 'ssh key with access nil, to project' do
      it 'should allow push access for host' do
        VCR.use_cassette("allowed-push") do
          access = gitlab_net.check_access('git-receive-pack', nil, project, key, changes, 'ssh')

          expect(access.allowed?).to be_truthy
          expect(access.gl_project_path).to eq('gitlab-org/gitlab.test')
        end
      end

      context 'but project not found' do
        where(:desc, :cassette, :message) do
          'deny push access for host'                                        | 'allowed-push-project-not-found'                | 'The project you were looking for could not be found.'
          'deny push access for host (when text/html)'                       | 'allowed-push-project-not-found-text-html'      | 'API is not accessible'
          'deny push access for host (when text/plain)'                      | 'allowed-push-project-not-found-text-plain'     | 'API is not accessible'
          'deny push access for host (when 404 is returned)'                 | 'allowed-push-project-not-found-404'            | 'The project you were looking for could not be found.'
          'deny push access for host (when 404 is returned with text/html)'  | 'allowed-push-project-not-found-404-text-html'  | 'API is not accessible'
          'deny push access for host (when 404 is returned with text/plain)' | 'allowed-push-project-not-found-404-text-plain' | 'API is not accessible'
        end

        with_them do
          it 'should deny push access for host' do
            VCR.use_cassette(cassette) do
              access = gitlab_net.check_access('git-receive-pack', nil, project, key, changes, 'ssh')
              expect(access.allowed?).to be_falsey
              expect(access.message).to eql(message)
            end
          end
        end
      end

      it 'adds the secret_token to the request' do
        VCR.use_cassette("allowed-push") do
          expect_any_instance_of(Net::HTTP::Post).to receive(:set_form_data).with(hash_including(secret_token: secret))
          gitlab_net.check_access('git-receive-pack', nil, project, key, changes, 'ssh')
        end
      end

      it 'should allow pull access for host' do
        VCR.use_cassette("allowed-pull") do
          access = gitlab_net.check_access('git-upload-pack', nil, project, key, changes, 'ssh')

          expect(access.allowed?).to be_truthy
          expect(access.gl_project_path).to eq('gitlab-org/gitlab.test')
        end
      end
    end

    context 'ssh access has been disabled' do
      it 'should deny pull access for host' do
        VCR.use_cassette('ssh-pull-disabled') do
          access = gitlab_net.check_access('git-upload-pack', nil, project, key, changes, 'ssh')
          expect(access.allowed?).to be_falsey
          expect(access.message).to eq 'Git access over SSH is not allowed'
        end
      end

      it 'should deny push access for host' do
        VCR.use_cassette('ssh-push-disabled') do
          access = gitlab_net.check_access('git-receive-pack', nil, project, key, changes, 'ssh')
          expect(access.allowed?).to be_falsey
          expect(access.message).to eq 'Git access over SSH is not allowed'
        end
      end
    end

    context 'http access has been disabled' do
      it 'should deny pull access for host' do
        VCR.use_cassette('http-pull-disabled') do
          access = gitlab_net.check_access('git-upload-pack', nil, project, key, changes, 'http')
          expect(access.allowed?).to be_falsey
          expect(access.message).to eq 'Pulling over HTTP is not allowed.'
        end
      end

      it 'should deny push access for host' do
        VCR.use_cassette("http-push-disabled") do
          access = gitlab_net.check_access('git-receive-pack', nil, project, key, changes, 'http')
          expect(access.allowed?).to be_falsey
          expect(access.message).to eq 'Pushing over HTTP is not allowed.'
        end
      end
    end

    context 'ssh key without access to project' do
      where(:desc, :cassette, :message) do
        'deny push access for host'                                        | 'ssh-push-project-denied'                | 'Git access over SSH is not allowed'
        'deny push access for host (when 401 is returned)'                 | 'ssh-push-project-denied-401'            | 'Git access over SSH is not allowed'
        'deny push access for host (when 401 is returned with text/html)'  | 'ssh-push-project-denied-401-text-html'  | 'API is not accessible'
        'deny push access for host (when 401 is returned with text/plain)' | 'ssh-push-project-denied-401-text-plain' | 'API is not accessible'
        'deny pull access for host'                                        | 'ssh-pull-project-denied'                | 'Git access over SSH is not allowed'
        'deny pull access for host (when 401 is returned)'                 | 'ssh-pull-project-denied-401'            | 'Git access over SSH is not allowed'
        'deny pull access for host (when 401 is returned with text/html)'  | 'ssh-pull-project-denied-401-text-html'  | 'API is not accessible'
        'deny pull access for host (when 401 is returned with text/plain)' | 'ssh-pull-project-denied-401-text-plain' | 'API is not accessible'
      end

      with_them do
        it 'should deny push access for host' do
          VCR.use_cassette(cassette) do
            access = gitlab_net.check_access('git-receive-pack', nil, project, key2, changes, 'ssh')
            expect(access.allowed?).to be_falsey
            expect(access.message).to eql(message)
          end
        end
      end

      it 'should deny pull access for host (with user)' do
        VCR.use_cassette("ssh-pull-project-denied-with-user") do
          access = gitlab_net.check_access('git-upload-pack', nil, project, 'user-2', changes, 'ssh')
          expect(access.allowed?).to be_falsey
          expect(access.message).to eql('Git access over SSH is not allowed')
        end
      end
    end

    it 'handles non 200 status codes' do
      resp = double(:resp, code: 501)

      allow(gitlab_net).to receive(:post).and_return(resp)

      access = gitlab_net.check_access('git-upload-pack', nil, project, 'user-2', changes, 'ssh')
      expect(access).not_to be_allowed
    end

    it "raises an exception if the connection fails" do
      allow_any_instance_of(Net::HTTP).to receive(:request).and_raise(StandardError)
      expect {
        gitlab_net.check_access('git-upload-pack', nil, project, 'user-1', changes, 'ssh')
      }.to raise_error(GitlabNet::ApiUnreachableError)
    end
  end

  describe '#base_api_endpoint' do
    let(:net) { described_class.new }

    subject { net.send :base_api_endpoint }

    it { is_expected.to include(net.send(:config).gitlab_url) }
    it("uses API version 4") { is_expected.to end_with("api/v4") }
  end

  describe '#internal_api_endpoint' do
    let(:net) { described_class.new }

    subject { net.send :internal_api_endpoint }

    it { is_expected.to include(net.send(:config).gitlab_url) }
    it("uses API version 4") { is_expected.to end_with("api/v4/internal") }
  end

  describe '#http_client_for' do
    subject { gitlab_net.send :http_client_for, URI('https://localhost/') }

    before do
      allow(gitlab_net).to receive :cert_store
      allow(gitlab_net.send(:config)).to receive(:http_settings) { {'self_signed_cert' => true} }
    end

    it { expect(subject.verify_mode).to eq(OpenSSL::SSL::VERIFY_NONE) }
  end

  describe '#http_request_for' do
    context 'with stub' do
      let(:get) { double(Net::HTTP::Get) }
      let(:user) { 'user' }
      let(:password) { 'password' }
      let(:url) { URI 'http://localhost/' }
      let(:params) { { 'key1' => 'value1' } }
      let(:headers) { { 'Content-Type' => 'application/json'} }
      let(:options) { { json: { 'key2' => 'value2' } } }

      context 'with no params, options or headers' do
        subject { gitlab_net.send :http_request_for, :get, url }

        before do
          allow(gitlab_net.send(:config).http_settings).to receive(:[]).with('user') { user }
          allow(gitlab_net.send(:config).http_settings).to receive(:[]).with('password') { password }
          expect(Net::HTTP::Get).to receive(:new).with('/', {}).and_return(get)
          expect(get).to receive(:basic_auth).with(user, password).once
          expect(get).to receive(:set_form_data).with(hash_including(secret_token: secret)).once
        end

        it { is_expected.not_to be_nil }
      end

      context 'with params' do
        subject { gitlab_net.send :http_request_for, :get, url, params: params, headers: headers }

        before do
          allow(gitlab_net.send(:config).http_settings).to receive(:[]).with('user') { user }
          allow(gitlab_net.send(:config).http_settings).to receive(:[]).with('password') { password }
          expect(Net::HTTP::Get).to receive(:new).with('/', headers).and_return(get)
          expect(get).to receive(:basic_auth).with(user, password).once
          expect(get).to receive(:set_form_data).with({ 'key1' => 'value1', secret_token: secret }).once
        end

        it { is_expected.not_to be_nil }
      end

      context 'with headers' do
        subject { gitlab_net.send :http_request_for, :get, url, headers: headers }

        before do
          allow(gitlab_net.send(:config).http_settings).to receive(:[]).with('user') { user }
          allow(gitlab_net.send(:config).http_settings).to receive(:[]).with('password') { password }
          expect(Net::HTTP::Get).to receive(:new).with('/', headers).and_return(get)
          expect(get).to receive(:basic_auth).with(user, password).once
          expect(get).to receive(:set_form_data).with(hash_including(secret_token: secret)).once
        end

        it { is_expected.not_to be_nil }
      end

      context 'with options' do
        context 'with json' do
          subject { gitlab_net.send :http_request_for, :get, url, options: options }

          before do
            allow(gitlab_net.send(:config).http_settings).to receive(:[]).with('user') { user }
            allow(gitlab_net.send(:config).http_settings).to receive(:[]).with('password') { password }
            expect(Net::HTTP::Get).to receive(:new).with('/', {}).and_return(get)
            expect(get).to receive(:basic_auth).with(user, password).once
            expect(get).to receive(:body=).with({ 'key2' => 'value2', secret_token: secret }.to_json).once
            expect(get).not_to receive(:set_form_data)
          end

          it { is_expected.not_to be_nil }
        end
      end
    end

    context 'Unix socket' do
      it 'sets the Host header to "localhost"' do
        gitlab_net = described_class.new
        expect(gitlab_net).to receive(:secret_token).and_return(secret)

        request = gitlab_net.send(:http_request_for, :get, URI('http+unix://%2Ffoo'))

        expect(request['Host']).to eq('localhost')
      end
    end
  end

  describe '#cert_store' do
    let(:store) do
      double(OpenSSL::X509::Store).tap do |store|
        allow(OpenSSL::X509::Store).to receive(:new) { store }
      end
    end

    before :each do
      expect(store).to receive(:set_default_paths).once
    end

    after do
      gitlab_net.send :cert_store
    end

    it "calls add_file with http_settings['ca_file']" do
      allow(gitlab_net.send(:config).http_settings).to receive(:[]).with('ca_file') { 'test_file' }
      allow(gitlab_net.send(:config).http_settings).to receive(:[]).with('ca_path') { nil }
      expect(store).to receive(:add_file).with('test_file')
      expect(store).not_to receive(:add_path)
    end

    it "calls add_path with http_settings['ca_path']" do
      allow(gitlab_net.send(:config).http_settings).to receive(:[]).with('ca_file') { nil }
      allow(gitlab_net.send(:config).http_settings).to receive(:[]).with('ca_path') { 'test_path' }
      expect(store).not_to receive(:add_file)
      expect(store).to receive(:add_path).with('test_path')
    end
  end
end
