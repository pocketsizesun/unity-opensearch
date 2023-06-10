# frozen_string_literal: true

module Unity
  module OpenSearch
    class BulkRequest
      IndexOperation = Struct.new(:index_name, :doc_id, :doc_attributes, :options)
      DeleteOperation = Struct.new(:index_name, :doc_id, :options)
      CreateOperation = Struct.new(:index_name, :doc_id, :doc_attributes, :options)
      UpdateOperation = Struct.new(:index_name, :doc_id, :doc_attributes, :options)

      def self.build(&_block)
        req = new
        yield(req)
        req
      end

      def initialize
        @operations = []
      end

      def index(index_name, doc_id, doc_attributes, **options)
        @operations << IndexOperation.new(index_name, doc_id, doc_attributes, options)
      end

      def delete(index_name, doc_id, **options)
        @operations << DeleteOperation.new(index_name, doc_id, options)
      end

      def create(index_name, doc_id, doc_attributes, **options)
        @operations << CreateOperation.new(index_name, doc_id, doc_attributes, options)
      end

      def update(index_name, doc_id, doc_attributes, **options)
        @operations << UpdateOperation.new(index_name, doc_id, doc_attributes, options)
      end

      def as_request_body
        arr = []
        @operations.each do |op|
          case op
          when IndexOperation
            arr << { 'index' => { '_index' => op.index_name, '_id' => op.doc_id }.merge!(op.options) }
            arr << op.doc_attributes
          when CreateOperation
            arr << { 'create' => { '_index' => op.index_name, '_id' => op.doc_id }.merge!(op.options) }
            arr << op.doc_attributes
          when UpdateOperation
            arr << { 'update' => { '_index' => op.index_name, '_id' => op.doc_id }.merge!(op.options) }
            arr << op.doc_attributes
          when DeleteOperation
            arr << { 'delete' => { '_index' => op.index_name, '_id' => op.doc_id }.merge!(op.options) }
          end
        end
        arr
      end
    end
  end
end
