require 'spec_helper'

require_relative '../../../lib/gitaly_server/sentry_interceptor.rb'
describe GitalyServer::SentryInterceptor do
  describe 'handling exceptions' do
    let(:meth) { GitalyServer::OperationsService.instance_method(:user_update_branch) }
    let(:ex) { ArgumentError.new("unknown encoding") }
    let(:call) { nil }

    subject do
      described_class.new.server_streamer(call: call, method: meth) { raise ex }
    end

    context 'Sentry is disabled' do
      it 're-raises the exception' do
        expect { subject }.to raise_error(ex)
      end

      it 'sends nothing to Sentry' do
        expect(Raven).not_to receive(:capture_exception)

        begin
          subject
        rescue
          nil
        end
      end
    end

    context 'Sentry is enabled' do
      before do
        allow(GitalyServer::Sentry).to receive(:enabled?).and_return(true)
      end

      let(:call_metadata) do
        {
          'user-agent' => 'grpc-go/1.9.1',
          'gitaly-storage-path' => '/home/git/repositories',
          'gitaly-repo-path' => '/home/git/repositories/gitlab-org/gitaly.git',
          'gitaly-gl-repository' => 'project-52',
          'gitaly-repo-alt-dirs' => ''
        }
      end
      let(:call) { double(metadata: call_metadata) }
      let(:expected_tags) do
        call_metadata.merge(
          'system' => 'gitaly-ruby',
          'gitaly-ruby.method' => 'GitalyServer::OperationsService#user_update_branch',
          Labkit::Correlation::CorrelationId::LOG_KEY => anything
        )
      end

      it 're-raises the exception' do
        expect { subject }.to raise_error(ex)
      end

      it 'sets Sentry tags' do
        expect(Raven).to receive(:tags_context).with(hash_including(expected_tags))

        begin
          subject
        rescue
          nil
        end
      end

      context 'when expcetion is normal' do
        it 'sends the exception to Sentry' do
          expect(Raven).to receive(:capture_exception).with(
            ex,
            fingerprint: ['gitaly-ruby', 'GitalyServer::OperationsService#user_update_branch', 'unknown encoding']
          )

          begin
            subject
          rescue
            nil
          end
        end
      end
    end
  end
end
