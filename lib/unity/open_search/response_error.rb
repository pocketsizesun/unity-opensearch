# frozen_string_literal: true

module Unity
  module OpenSearch
    class ResponseError < Error
      # @return [HTTP::Response]
      attr_reader :response

      # @return [Hash{String => Object}, nil]
      attr_reader :data

      # @param response [HTTP::Response]
      def initialize(response)
        # @type [HTTP::Response]
        @response = response

        # @type [Hash{String => Object}, nil]
        @data = \
          begin
            @response.parse(:json)
          rescue JSON::ParserError
            nil
          end

          super("response error [#{response.code}]: #{response.body}")
      end

      # @return [String, nil]
      def reason
        @data&.dig('reason')
      end

      def code
        @response.code
      end
    end
  end
end
