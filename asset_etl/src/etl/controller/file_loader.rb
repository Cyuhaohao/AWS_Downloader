
module Puzzle
  module AssetETL

    class FileLoader
      def initialize(options = {})
        @options = options

        @io = nil
      end

      def each_message
        open

        read do |message|
          yield message if block_given?
        end
      ensure
        close
      end

      private

      def open
        @io = File.open(@options[:filename], 'r:UTF-8')

        raise "Failed to open file #{@options[:filename]}" unless @io
      end

      def close
        @io.close if @io
      end

      def read
        loop do
          line = @io.gets
          break unless line
          yield line.chomp if block_given?
        end
      end
    end
  end


end
