# frozen_string_literal: true

module Unity
  module OpenSearch
    class Client
      # @return [Integer]
      attr_accessor :retry_max_count

      # @return [Integer]
      attr_accessor :retry_sleep_interval

      # @return [HTTP::Client]
      attr_reader :client

      METHOD_GET    = :get
      METHOD_POST   = :post
      METHOD_DELETE = :delete
      METHOD_PUT    = :put

      NEW_LINE = "\n"
      BULK_HTTP_HEADERS = {
        'content-type' => 'application/json'
      }.freeze

      DEFAULT_PARAMETERS = {}.freeze

      # @param url [String]
      # @param http_connect_timeout [Integer]
      # @param http_write_timeout [Integer]
      # @param http_read_timeout [Integer]
      # @param retry_max_count [Integer]
      # @param retry_sleep_interval [Integer]
      def initialize(url = 'http://localhost:9200', **kwargs)
        # @type [Integer]
        @retry_max_count = kwargs[:retry_max_count] || 3
        # @type [Integer]
        @retry_sleep_interval = kwargs[:retry_sleep_interval] || 1

        # @type [HTTP::Client]
        @client = HTTP.timeout(
          connect: kwargs[:http_connect_timeout] || 10,
          write: kwargs[:http_write_timeout] || 10,
          read: kwargs[:http_read_timeout] || 10
        ).persistent(url)
      end

      # @param method [Symbol]
      # @param path [String]
      # @raise [HTTP::Error]
      # @raise [Unity::OpenSearch::ResponseError]
      # @return [Hash{String => Object}]
      def request(method, path, **kwargs)
        retries_count = 0

        begin
          resp = @client.request(method, path, **kwargs).flush

          unless resp.status.success?
            case resp.code
            when 400
              raise Unity::OpenSearch::Errors::BadRequestError.new(resp)
            when 404
              raise Unity::OpenSearch::Errors::NotFoundError.new(resp)
            when 409
              raise Unity::OpenSearch::Errors::ConflictError.new(resp)
            when 500
              raise Unity::OpenSearch::Errors::InternalServerError.new(resp)
            else
              raise Unity::OpenSearch::ResponseError.new(resp)
            end
          end

          resp.parse(:json)
        rescue HTTP::ConnectionError => e
          raise e if retries_count >= @retry_max_count

          retries_count += 1
          sleep @retry_sleep_interval
          retry
        end
      end

      # @param index_name [String]
      # @param body [Hash{String => Object}]
      # @return [Hash{String => Object}]
      def search(index_name, body = {}, **kwargs)
        request(
          METHOD_GET, "/#{index_name}/_search",
          params: kwargs[:parameters] || DEFAULT_PARAMETERS,
          json: body
        )
      end

      # @param index_name [String]
      # @param body [Hash{String => Object}]
      # @return [Hash{String => Object}]
      def count(index_name, body = {}, **kwargs)
        request(
          METHOD_GET, "/#{index_name}/_count",
          params: kwargs[:parameters] || DEFAULT_PARAMETERS,
          json: body
        )
      end

      # @param index_name [String]
      # @param doc_id [String]
      # @param doc_attributes [Hash{String => Object}]
      # @return [Hash{String => Object}]
      def index(index_name, doc_id, doc_attributes, **kwargs)
        request(
          METHOD_POST, "/#{index_name}/_doc/#{doc_id}",
          params: kwargs[:parameters] || DEFAULT_PARAMETERS,
          json: doc_attributes
        )
      end

      # @param index_name [String]
      # @param doc_id [String]
      # @return [Hash{String => Object}]
      def delete(index_name, doc_id, **kwargs)
        request(
          METHOD_DELETE, "/#{index_name}/_doc/#{doc_id}",
          params: kwargs[:parameters] || DEFAULT_PARAMETERS
        )
      end

      # @param index_name [String]
      # @param doc_id [String]
      # @param doc_attributes [Hash{String => Object}]
      # @return [Hash{String => Object}]
      def update(index_name, doc_id, doc_attributes, **kwargs)
        request(
          METHOD_POST, "/#{index_name}/_update/#{doc_id}",
          params: kwargs[:parameters] || DEFAULT_PARAMETERS,
          json: { 'doc' => doc_attributes }
        )
      end

      # @param request [Unity::OpenSearch::BulkRequest]
      # @return [Hash{String => Object}]
      def bulk(request, **kwargs)
        request(
          METHOD_POST, '/_bulk',
          headers: BULK_HTTP_HEADERS,
          params: kwargs[:parameters] || DEFAULT_PARAMETERS,
          body: request.as_request_body.collect(&:to_json).join(NEW_LINE) + NEW_LINE
        )
      end
    end
  end
end
