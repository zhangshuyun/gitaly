module Praefect
  class Transaction
    TRANSACTION_METADATA_KEY = "gitaly-reference-transaction".freeze
    TRANSACTION_PAYLOAD_KEY = "transaction".freeze

    MissingPraefectMetadataError = Class.new(StandardError)

    def self.from_metadata(metadata)
      transaction_metadata = metadata[TRANSACTION_METADATA_KEY]
      return new(nil) unless transaction_metadata

      transaction = JSON.parse(Base64.decode64(transaction_metadata))

      new(transaction)
    end

    def initialize(transaction)
      @transaction = transaction
    end

    def payload
      {
        TRANSACTION_PAYLOAD_KEY => @transaction
      }.reject { |_, v| v.nil? }
    end
  end
end
