require 'json'

module Puzzle
  module AssetETL

    class AssetParser

      def initialize(options = {})
        @options = options
      end

      def parse(line)
        record_time = 0
        json_data = nil

        begin
          lineArray = line.split(' ', 2);

          record_time = Integer(lineArray[0])

          json_data = JSON.parse(lineArray[1])

        rescue
          raise "parse asset json error on " + lineArray[1]
        end

        return record_time, json_data
      end

    end

  end
end
