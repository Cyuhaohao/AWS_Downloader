require_relative '../interface/db_client'
require 'mysql2'


module Puzzle

  module Packages

    class MysqlClient < IDBClient
      DEFAULT_OPTIONS = {
          host: nil,
          port: nil,
          username: nil,
          password: nil,
          database: nil,
      }

      DATA_TYPE_SUPPORTED_LIST = {
          int: 'int(11)',
          tinyint: 'tinyint(4)',
          smallint: 'smallint(6)',
          bigint: 'bigint(20)',
          boolean: 'tinyint(1)',

          ascii: 'varchar(255)',
          uuid: 'char(36)',
          text: 'varchar(255)',
          longascii: 'text',
          longtext: 'text',

          float: 'float',
          double: 'double',
          currency: 'decimal(10,4)',

          datetime: 'datetime',
          timestamp: 'int(11)',
          timestamp_ms: 'bigint(20)',
      }

      def initialize(options = nil)
        @options = DEFAULT_OPTIONS.clone
        @options.merge!(options) if options.is_a?(Hash)

        @mysql = nil
      end

      def connect()
        return false if @mysql
        @mysql = Mysql2::Client.new(@options)
        return @mysql ? true : false
      end

      def close()
        return unless @mysql
        @mysql.close
        @mysql = nil
      end

      def execute(sql)
        return unless @mysql
        return @mysql.query(sql)
      end

      def prepare(sql)
        return unless @mysql
        return @mysql.prepare(sql)
      end

      def stmt_execute(stmt, args)
        return unless @mysql
        return unless stmt
        return stmt.execute(*args)
      end

      def get_supported_type(type)
        DATA_TYPE_SUPPORTED_LIST[type.to_sym]
      end

      def get_data_value(data, type)
        return unless data
        return unless get_supported_type(type)

        case type
        when 'uuid'
          str = data.gsub(/-/, '')
          raise ::ArgumentError, "Expected 32 hexadecimal digits but got #{str.length}" unless str.length == 32
        when 'timestamp'
          raise ::ArgumentError, "Invalid value for timestamp #{data}" unless data.is_a?(Integer)
          data = data / 1000 if data > 0xffffffff
        when 'timestamp_ms'
          raise ::ArgumentError, "Invalid value for timestamp #{data}" unless data.is_a?(Integer)
          data = data * 1000 unless data > 0xffffffff
        end

        data
      end
    end

  end

end
