require_relative '../interface/db_client'
require 'cassandra'

module GreatEye

  class CassandraClient < IDBClient
    DEFAULT_OPTIONS = {
      hosts: nil,
      port: nil,
      username: nil,
      password: nil,
      keyspace: nil,
    }

    DATA_TYPE_SUPPORTED_LIST = {
      int: 'int',
      tinyint: 'tinyint',
      smallint: 'smallint',
      bigint: 'bigint',
      boolean: 'boolean',

      ascii: 'ascii',
      uuid: 'uuid',
      text: 'text',
      longascii: 'ascii',
      longtext: 'text',

      float: 'float',
      double: 'double',
      currency: 'decimal',

      datetime: 'timestamp',
      timestamp: 'timestamp',
      timestamp_ms: 'timestamp',
    }

    def initialize(options = nil)
      @options = DEFAULT_OPTIONS.clone
      @options.merge!(options) if options.is_a?(Hash)

      @cluster = nil
      @session = nil
    end

    def connect()
      return false if @cluster
      @cluster = Cassandra.cluster(@options)
      @session = @cluster.connect(@options[:keyspace])
      return (@cluster or @session) ? true : false
    end

    def close()
      return unless @cluster
      @cluster.close
      @cluster = nil
      @session = nil
    end

    def execute(sql)
      return unless @session
      return @session.execute(sql)
    end

    def prepare(sql)
      return unless @session
      return @session.prepare_async(sql).get
    end

    def stmt_execute(stmt, args)
      return unless @session
      return unless stmt
      return @session.execute(stmt, arguments: args)
    end

    def stmt_execute_async(stmt, args)
      return unless @session
      return unless stmt
      return @session.execute_async(stmt, arguments: args)
    end

    def stmt_result_join(future)
      return unless @session
      return unless future and future.is_a?(Cassandra::Future)
      return future.join
    end

    def get_supported_type(type)
      DATA_TYPE_SUPPORTED_LIST[type.to_sym]
    end

    def get_data_value(data, type)
      return unless data
      return unless get_supported_type(type)

      case type
        when 'uuid'
          data = Cassandra::Uuid.new(data)
        when 'float'
          data = data.to_f if data.is_a?(Integer)
        when 'double'
          data = data.to_f if data.is_a?(Integer)
        when 'currency'
          data = data.to_s unless data.is_a?(String)
          data = BigDecimal.new(data)
        when 'datetime'
          data = Time.new(data)
        when 'timestamp'
          raise ::ArgumentError, "Invalid value for timestamp #{data}" unless data.is_a?(Integer)
          data = Foundation::Util.timestamp_to_time(data)
        when 'timestamp_ms'
          raise ::ArgumentError, "Invalid value for timestamp #{data}" unless data.is_a?(Integer)
          data = Foundation::Util.timestamp_in_ms_to_time(data)
      end

      data
    end
  end

end
