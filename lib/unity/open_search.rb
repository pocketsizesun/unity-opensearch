# frozen_string_literal: true

require 'http'

require_relative 'open_search/bulk_request'
require_relative 'open_search/client'
require_relative 'open_search/error'
require_relative 'open_search/response_error'
require_relative 'open_search/errors/bad_request_error'
require_relative 'open_search/errors/conflict_error'
require_relative 'open_search/errors/not_found_error'
require_relative 'open_search/errors/internal_server_error'
require_relative 'open_search/version'

module Unity
  module OpenSearch
  end
end
