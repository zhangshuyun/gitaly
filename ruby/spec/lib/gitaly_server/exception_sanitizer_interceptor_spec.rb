require 'spec_helper'

require_relative '../../../lib/gitaly_server/exception_sanitizer_interceptor.rb'
describe GitalyServer::ExceptionSanitizerInterceptor do
  let(:meth) { GitalyServer::RefService.instance_method(:create_branch) }
  let(:ex) { StandardError.new("error: failed to push some refs to 'https://fO0BA7:HunTer!@github.com/ruby/ruby.git'") }
  let(:ex_sanitized_message) { "error: failed to push some refs to 'https://[FILTERED]@github.com/ruby/ruby.git'" }
  let(:call) { double(metadata: {}) }

  subject do
    described_class.new.server_streamer(call: call, method: meth) { raise ex }
  end

  context 'normal exception' do
    it 'sanitizes exception message' do
      expect { subject }.to raise_error(ex_sanitized_message)
    end
  end

  context 'with incomplete url in exception' do
    let(:ex) { "unable to look up user:pass@non-existent.org (port 9418)" }
    let(:ex_sanitized_message) { "unable to look up [FILTERED]@non-existent.org (port 9418)" }

    it 'sanitizes exception message' do
      expect { subject }.to raise_error(ex_sanitized_message)
    end
  end

  context 'GRPC::BadStatus exception' do
    let(:ex) { GRPC::Unknown.new(super().message) }

    it 'sanitizes exception message and details' do
      rescued_ex = nil

      begin
        subject
      rescue => e
        rescued_ex = e
      end

      expect(rescued_ex.message).to end_with(ex_sanitized_message)
      expect(rescued_ex.details).to eq(ex_sanitized_message)
    end
  end
end
