# frozen_string_literal: true

require 'spec_helper'

describe Gitlab::Git::RepositoryMirroring do
  class FakeRepository
    include Gitlab::Git::RepositoryMirroring

    def initialize(projects_stub)
      @gitlab_projects = projects_stub
    end

    def gitlab_projects_error
      raise Gitlab::Git::CommandError, @gitlab_projects.output
    end
  end

  describe '#push_remote_branches' do
    let(:projects_stub) { double.as_null_object }

    subject(:repository) { FakeRepository.new(projects_stub) }

    context 'with a successful first push' do
      it 'returns true' do
        expect(projects_stub).to receive(:push_branches)
          .with('remote_a', anything, true, %w[master], env: {})
          .once
          .and_return(true)

        expect(projects_stub).not_to receive(:output)

        expect(repository.push_remote_branches('remote_a', %w[master])).to eq(true)
      end
    end

    context 'with a failed push and disabled retries' do
      before do
        allow(projects_stub)
          .to receive(:feature_flags)
          .and_return(double(enabled?: false))
      end

      it 'logs a list of branch results and raises CommandError' do
        output = "Oh no, push mirroring failed!"
        logger = spy

        # Once for parsing, once for the exception
        allow(projects_stub).to receive(:output).twice.and_return(output)
        allow(projects_stub).to receive(:logger).and_return(logger)

        push_results = double(
          accepted_refs: %w[develop],
          rejected_refs: %w[master]
        )
        expect(Gitlab::Git::PushResults).to receive(:new)
          .with(output)
          .and_return(push_results)

        # Push fails
        expect(projects_stub).to receive(:push_branches).and_return(false)

        # The CommandError gets raised, matching existing behavior
        expect { repository.push_remote_branches('remote_a', %w[master develop]) }
          .to raise_error(Gitlab::Git::CommandError, output)

        # Ensure we logged a message with the PushResults info
        expect(logger).to have_received(:info).with(%r{Accepted: develop / Rejected: master})
      end
    end

    context 'with a failed push and enabled retries' do
      let(:feature_flags) do
        GitalyServer::FeatureFlags.new('gitaly-feature-ruby-push-mirror-retry' => 'true')
      end

      before do
        expect(projects_stub).to receive(:feature_flags).and_return(feature_flags)
      end

      it 'retries with accepted refs' do
        output = "Oh no, push mirroring failed!"

        # Once for parsing, once for the exception
        allow(projects_stub).to receive(:output).and_return(output)

        push_results = double(
          accepted_refs: %w[develop],
          rejected_refs: %w[master]
        )
        expect(Gitlab::Git::PushResults).to receive(:new)
          .with(output)
          .and_return(push_results)

        # First push fails
        expect(projects_stub)
          .to receive(:push_branches)
          .with('remote_a', anything, true, %w[master develop], env: {})
          .and_return(false)

        # Second push still fails, but we tried!
        expect(projects_stub)
          .to receive(:push_branches)
          .with('remote_a', anything, true, %w[develop], env: {})
          .and_return(false)

        # The CommandError gets raised, matching existing behavior
        expect { repository.push_remote_branches('remote_a', %w[master develop]) }
          .to raise_error(Gitlab::Git::CommandError, output)
      end

      it 'does nothing with no accepted refs' do
        push_results = double(
          accepted_refs: [],
          rejected_refs: %w[master develop]
        )
        expect(Gitlab::Git::PushResults).to receive(:new).and_return(push_results)

        # Nothing was accepted, so we only push once
        expect(projects_stub).to receive(:push_branches).once.and_return(false)

        # The CommandError gets raised, matching existing behavior
        expect { repository.push_remote_branches('remote_a', %w[master develop]) }
          .to raise_error(Gitlab::Git::CommandError)
      end
    end
  end
end
