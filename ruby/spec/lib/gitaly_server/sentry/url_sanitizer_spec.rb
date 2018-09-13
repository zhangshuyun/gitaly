require 'spec_helper'

require 'raven/base'
require 'raven/transports/dummy'
require_relative '../../../../lib/gitaly_server/sentry.rb'

# rubocop:disable Lint/RescueWithoutErrorClass
describe GitalyServer::Sentry::URLSanitizer do
  let(:meth) { GitalyServer::RefService.instance_method(:create_branch) }
  let(:ex) { StandardError.new("error: failed to push some refs to 'https://fO0BA7:HunTer!@github.com/ruby/ruby.git'") }
  let(:ex_sanitized_message) { "error: failed to push some refs to 'https://[FILTERED]@github.com/ruby/ruby.git'" }
  let(:call) { double(metadata: {}) }

  before do
    Raven.configure do |config|
      config.dsn = "dummy://12345:67890@sentry.localdomain:3000/sentry/42"
      config.encoding = 'json'
    end

    allow(GitalyServer::Sentry).to receive(:enabled?).and_return(true)
  end

  it 'sanitizes exception data' do
    begin
      GitalyServer::SentryInterceptor.new.server_streamer(call: call, method: meth) { raise ex }
    rescue
      nil
    end

    data = JSON.parse(last_sentry_event[1])

    expect(data['message']).to eq("StandardError: #{ex_sanitized_message}")
    expect(data['logentry']['message']).to eq("StandardError: #{ex_sanitized_message}")
    expect(data['fingerprint'].last).to eq(ex_sanitized_message)
    expect(data['exception']['values'][0]['value']).to eq(ex_sanitized_message)
  end

  context 'muliple exception causes' do
    it 'sanitizes all exceptions' do
      cause = StandardError.new("Authorization failed for 'https://fA0TAQ:h2nTer!@github.com/ruby/ruby.git', sorry!")
      cause_sanitized_message = "Authorization failed for 'https://[FILTERED]@github.com/ruby/ruby.git', sorry!"

      begin
        GitalyServer::SentryInterceptor.new.server_streamer(call: call, method: meth) do
          begin
            raise cause
          rescue
            raise ex
          end
        end
      rescue
        nil
      end

      data = JSON.parse(last_sentry_event[1])

      expect(data['exception']['values'][0]['value']).to eq(cause_sanitized_message)
      expect(data['exception']['values'][1]['value']).to eq(ex_sanitized_message)
    end
  end

  def last_sentry_event
    Raven.instance.client.transport.events.last
  end
end
