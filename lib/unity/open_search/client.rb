# frozen_string_literal: true

module Unity
  module OpenSearch
    class Client
      METHOD_GET    = :get
      METHOD_POST   = :post
      METHOD_DELETE = :delete
      METHOD_PUT    = :put

      DEFAULT_PARAMETERS = {}.freeze

      # @param url [String]
      # @param http_connect_timeout [Integer]
      # @param http_write_timeout [Integer]
      # @param http_read_timeout [Integer]
      def initialize(url = 'http://localhost:9200', **kwargs)
        @client = HTTP.timeout(
          connect: kwargs[:http_connect_timeout] || 15,
          write: kwargs[:http_write_timeout] || 15,
          read: kwargs[:http_read_timeout] || 15
        ).persistent(url)
      end

      def client
        @client
      end

      # @param method [Symbol]
      # @param path [String]
      # @return [Hash{String => Object}]
      def request(method, path, **kwargs)
        resp = @client.request(method, path, **kwargs).flush

        unless resp.status.success?
          case resp.code
          when 404
            raise Unity::OpenSearch::Errors::NotFoundError.new(resp)
          when 409
            raise Unity::OpenSearch::Errors::ConflictError.new(resp)
          else
            raise Unity::OpenSearch::ResponseError.new(resp)
          end
        end

        resp.parse(:json)
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
          METHOD_PUT, "/#{index_name}/_doc/#{doc_id}",
          params: kwargs[:parameters] || DEFAULT_PARAMETERS,
          json: { 'doc' => doc_attributes }
        )
      end

      # @param operations [Array<Hash{String => Object}>]
      # @return [Hash{String => Object}]
      def bulk(operations, **kwargs)
        request(
          METHOD_POST, '/_bulk',
          params: kwargs[:parameters] || DEFAULT_PARAMETERS,
          json: operations
        )
      end
    end
  end
end
