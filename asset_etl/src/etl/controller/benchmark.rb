module Puzzle
  module AssetETL
    module Benchmark

      $benchmark_file = 'benchmark.log'

      def init_benchmark(out_path)
        # if out_path
        #   FileUtils.mkdir_p(out_path) unless Dir.exist?(out_path)
        #   $benchmark_file = File.join(out_path, $benchmark_file) if out_path
        # end
      end

      def time_elapsed(message=nil, fatal_elapsed=nil, error_elapsed=nil, warning_elapsed=nil, trace_elapsed=nil)
        elapsed = 0 # ms

        begin
          t = Time.now
          yield if block_given?
          elapsed = ((Time.now - t) * 1000).to_i

          message = '' unless message
          fatal_elapsed = 30000 unless fatal_elapsed
          error_elapsed = 10000 unless error_elapsed
          warning_elapsed = 3000 unless warning_elapsed
          trace_elapsed = 1000 unless trace_elapsed

          cur_time = Time.now.strftime('%Y-%m-%d %H:%M:%S %z')

          log_message = nil

          if elapsed > fatal_elapsed
            log_message = "[#{cur_time}][FATAL]|TimeElapsed:#{elapsed}ms|#{message}"
            puts log_message
          elsif elapsed > error_elapsed
            log_message = "[#{cur_time}][ERROR]|TimeElapsed:#{elapsed}ms|#{message}"
            puts log_message
          elsif elapsed > warning_elapsed
            log_message = "[#{cur_time}][WARN]|TimeElapsed:#{elapsed}ms|#{message}"
            puts log_message
          elsif elapsed > trace_elapsed
            log_message = "[#{cur_time}][TRACK]|TimeElapsed:#{elapsed}ms|#{message}"
            puts log_message
            log_message = nil
          end

          # if log_message
          #   File.open($benchmark_file, 'a') { |f| f.puts(log_message) }
          # end
        ensure
        end
      end

    end
  end
end
