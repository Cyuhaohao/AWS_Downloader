require 'pathname'
require 'yaml'

require_relative 'controller/file_loader'
require_relative 'controller/aws_s3_loader'
require_relative 'controller/asset_parser'
require_relative 'controller/mysql_storer'
require_relative '../packages/foundation/autorun'

module Puzzle
  module AssetETL

    class ETLApp
      @root_path = nil
      @config = nil
      @loader = nil
      @parser = nil
      @storer = nil

      def initialize(config_file)
        @config = YAML.load(File.open(config_file)).symbolize_keys

        loader_options = {
            config: @config,
            filename: 'D:\\flm_workspace\\Puzzle\\asset_etl\\data\\qaqc-20190702010.log'
        }
        # @loader = FileLoader.new(loader_options)

        @loader = AwsS3Loader.new(@config)
        @parser = AssetParser.new({})
        @storer = MysqlStorer.new(@config)
      end

      def close()
        @storer.send(:close)
      end

      def run()

        dataArray = []

        @loader.each_message do |line|
          data = {
            file_path: '',
            asset_index: '',
            message: '',
            record_time: 0,
            json_data: nil
          }

          begin

            if line.is_a?(String)
              data[:file_path] = '/2019/07/03/dev/filepath'
              data[:asset_index] = 'zipfilename:123'
              data[:message] = line
              data[:record_time], data[:json_data] = @parser.parse(line)
            else
              data[:file_path] = line[:file_path]
              data[:asset_index] = line[:asset_index]
              data[:message] = line[:message]
              data[:record_time], data[:json_data] = @parser.parse(line[:message].chomp)
            end

          rescue Exception => e
            puts e
            next
          end


          @storer.save(data)
        end

        # save last data
        @storer.send(:saveArray)
        close
      end

    end

  end
end
