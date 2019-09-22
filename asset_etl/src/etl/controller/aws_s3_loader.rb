require 'aws-sdk-s3'
require 'fileutils'
require 'zip'
require 'zlib'
require 'time'
require_relative 'benchmark'

module Puzzle
  module AssetETL

    class AwsS3Loader
      include Puzzle::AssetETL::Benchmark

      def initialize(options = {})
        @options = options

        @env = options[:game_env]

        # @node_count = options[:game_config][:node_count] || 1
        # @node_index = options[:game_config][:node_index] || 0

        @cursor_file = "cursor_#{@env}.yml"
        @cursor_bak_fmt = "cursor_#{@env}_{start}_{end}.yml"

        # if @node_count > 1
        #   @cursor_file = "cursor_#{@env}_#{@node_count}-#{@node_index+1}.yml"
        #   @cursor_bak_fmt = "cursor_#{@env}_#{@node_count}-#{@node_index+1}_{start}_{end}.yml"
        # end

        aws_s3 = options[:aws_s3]
        @s3_bucket = aws_s3[:s3_config][:s3_bucket]
        @s3 = Aws::S3::Client.new(aws_s3[:s3_config][:s3_client_config])

        @remote_path = aws_s3[:remote_path].gsub(/{env}/, @env)
        @local_path = aws_s3[:local_path].gsub(/{env}/, @env)
        @refresh_interval = aws_s3[:refresh_interval] || 0

        # unless options[:game_config][:game_s3_path].blank?
        #   @remote_path = options[:game_config][:game_s3_path]
        # end

        FileUtils.mkdir_p(@local_path) unless Dir.exist?(@local_path)

        @cursors_file = File.join(@local_path, @cursor_file)
        @cursors_data = nil

        init_benchmark(@local_path)

        init_cursors
      end

      def each_message
        # while true
          prefixes = list_prefixes

          finished_keys = @cursors_data[:finished_keys]

          prefixes.each do |prefix|
            files = list_files(prefix)

            key = prefix.split('/').last(3).join('/').to_sym

            if files.empty?
              if prefix != prefixes[-1]
                finished_keys[key] = {}
                save_cursors
              end
            else
              files.each do |filename|
                finished_files = finished_keys[key] || {}
                next if finished_files[filename.to_sym]

                read_file_gzip(prefix, filename) do |message|
                  time_elapsed("aws_s3_loader::each_message() - #{message}") do
                    yield message if block_given?
                  end
                end
              end
            end
          end

          sleep(@refresh_interval) if @refresh_interval > 0
        # end
      ensure
        # close
      end

      private

      def init_cursors
        @cursors_data = YAML.load_file(@cursors_file) if File.exist?(@cursors_file)

        unless @cursors_data
          @cursors_data = {
            bucket: @s3_bucket,
            prefix: @remote_path,
            modified_time: 0,
            next_key: '',
            finished_keys: {},
          }
          save_cursors
        end

        @cursors_data[:finished_keys] = {} unless @cursors_data[:finished_keys]
      end

      def save_cursors
        @cursors_data[:modified_time] = Time.now.to_i

        finished_keys = @cursors_data[:finished_keys]
        if finished_keys.length >= 2 + 14
          target_finished_keys = {}

          prefixes = finished_keys.keys.sort
          [prefixes[-2], prefixes[-1]].each do |key|
            target_finished_keys[key] = finished_keys[key]
            finished_keys.delete(key)
          end

          start_key = prefixes[0].to_s.gsub(/\//, '-')
          end_key = prefixes[-3].to_s.gsub(/\//, '-')

          cursors_file = @cursors_file.chomp(@cursor_file)
          cursors_file += @cursor_bak_fmt.gsub(/{start}/, start_key).gsub(/{end}/, end_key)

          File.open(cursors_file, 'w') do |f|
            f.write @cursors_data.to_yaml
          end

          @cursors_data[:finished_keys] = target_finished_keys
        end

        File.open(@cursors_file, 'w') do |f|
          f.write @cursors_data.to_yaml
        end
      rescue => e
      end

      def list_prefixes
        start_after, last_key = next_prefix_keys
        # start_after = 'AssetUsage/qaqc/2019/06/28'
        # last_key = start_after

        # data = nil
        #
        # time_elapsed("aws_s3_loader::list_prefixes() - s3:list_objects_v2, prefix:'#{@remote_path}', start_after:'#{start_after}'") do
        #   data = @s3.list_objects_v2({
        #     bucket: @s3_bucket,
        #     delimiter: '/',
        #     prefix: @remote_path + '/',
        #     start_after: start_after,
        #   })
        # end
        #
        # return if data.nil?

        prefixes = []
        prefixes.push(start_after) unless start_after == ''
        prefixes.push(last_key) unless last_key == ''

        prefixes = prefixes.uniq

        # unless data.nil?
        #   data.common_prefixes.each do |prefix|
        #     prefixes.push(prefix.prefix) unless prefixes.include?(prefix.prefix)
        #   end
        # end

        operate_prefixes(prefixes)
      end

      def operate_prefixes(prefixes)

        if prefixes.empty?
          @cursors_data[:next_key] = get_first_key
          prefixes = [@remote_path + '/' + @cursors_data[:next_key] + '/']
          save_cursors
        end

        start_date = prefixes[-1].split('/').last(3).join('/')
        today = Time.now.utc.strftime('%Y/%m/%d')

        while start_date<today
          start_date = (Time.parse(start_date) + 3600*24).strftime('%Y/%m/%d')
          prefixes.push(@remote_path + '/' + start_date + '/')
        end

        prefixes
      end

      def get_first_key
        data = nil
        current_path = ''

        for i in 0..2
          time_elapsed("aws_s3_loader::list_prefixes() - s3:list_objects_v2, prefix:'#{@remote_path}', start_after:''") do
            data = @s3.list_objects_v2({
              bucket: @s3_bucket,
              delimiter: '/',
              prefix: @remote_path + '/' + current_path,
              start_after: '',
            })
          end

          raise "AWS is empty" if data.empty?

          if data.common_prefixes.empty?
            raise "No files found in AWS document"
          end

          data.common_prefixes.each do |file_name|
            file_name_end = file_name.prefix.split('/')[-1]
            if file_name_end.to_i.to_s.rjust(4, '0') == file_name_end.rjust(4, '0')
              current_path += file_name_end + '/'
              break
            end
          end
        end

        begin
          Time.parse(current_path[0..-2])
        rescue
          raise "Form of filename is incorrect"
        end

        current_path[0..-2]
      end

      def list_files(prefix)
        files = []

        start_after = ''
        while true
          data = nil

          time_elapsed("aws_s3_loader::list_files() - s3:list_objects_v2, prefix:'#{prefix}', start_after:'#{start_after}'") do
            data = @s3.list_objects_v2({
              bucket: @s3_bucket,
              prefix: prefix,
              start_after: start_after,
            })
          end

          break if data.nil?

          if data.key_count > 0
            key = prefix.gsub(@remote_path + '/', '')[0...-1]
            # finished_files = (@cursors_data[:finished_keys][key.to_sym] or {})
            # finished_filenames = finished_files.keys

            data.contents.each do |content|
              content_prefix = "#{@remote_path}/#{key}/"
              content_filename = content.key.gsub(content_prefix, '')
              files.push(content_filename) # unless finished_filenames.include?(content_filename.to_sym)
            end

            start_after = data.contents[-1].key if (data.key_count > 0)
          end

          break unless data.is_truncated
        end

        files
      end

      def next_prefix_keys
        finished_keys = @cursors_data[:finished_keys]
        prefixes = finished_keys.keys.sort

        saved_key = @cursors_data[:next_key]
        unless saved_key
          saved_key = ''
          @cursors_data[:next_key] = ''
        end

        if prefixes.length == 0
          next_key = "#{@remote_path}/#{saved_key}/" unless saved_key == ''
          return [next_key || '', '']
        end

        last_key = prefixes[-1].to_s if prefixes.length > 0
        next_key = prefixes[-2].to_s if prefixes.length > 1
        next_key = last_key unless next_key

        if saved_key != ''
          if next_key.nil? || next_key < saved_key
            next_key = saved_key
          end
        end

        last_key = '' if last_key && last_key < next_key
        last_key = '' unless last_key

        @cursors_data[:next_key] = next_key

        next_key = "#{@remote_path}/#{next_key}/" unless next_key == ''
        last_key = "#{@remote_path}/#{last_key}/" unless last_key == ''

        [next_key, last_key]
      end

      def read_file(prefix, filename)
        data = @s3.get_object({
          bucket: @s3_bucket,
          key: prefix + filename,
        })

        prefix_key = prefix.gsub(@remote_path + '/', '')[0...-1]

        Zip::File.open_buffer(data.body) do |zip_file|
          file_list = {}

          zip_file.each do |entry|
            line_count = 0

            read_zis(entry.get_input_stream) do |message|
              break unless message
              # puts '[[' + filename + ' |> ' + entry.name + ']]: ' + message

              line_count = line_count + 1
              yield message if block_given?
            end

            file_list[entry.name.to_sym] = {
              size: entry.size,
              compressed_size: entry.compressed_size,
              time: entry.time,
              crc: entry.crc,
              line_count: line_count,
            }
          end

          file_info = {
            key: prefix + filename,
            content_length: data.content_length,
            last_modified: data.last_modified,
            etag: data.etag,
            finish_time: Time.now.to_i,
            file_list: file_list,
          }

          finish_file(prefix_key, filename, file_info)
        end
      end

      def read_file_gzip(prefix, filename)
        data = @s3.get_object({
          bucket: @s3_bucket,
          key: prefix + filename,
        })

        prefix_key = prefix.gsub(@remote_path + '/', '')[0...-1]

        line_count = 0

        zip_file = Zlib::GzipReader.new(data.body)
        zip_file.each_line do |message|
          break unless message
          # puts '[[' + filename + ' |> ' + entry.name + ']]: ' + message

          line_count = line_count + 1
          line = {
              message: message,
              file_path: prefix,
              asset_index: "#{filename}:%05d" % line_count,
          }
          yield line if block_given?
        end

        file_info = {
          key: prefix + filename,
          content_length: data.content_length,
          last_modified: data.last_modified,
          etag: data.etag,
          finish_time: Time.now.to_i,
          gzip: {
            #
            line_count: line_count,
          },
        }

        finish_file(prefix_key, filename, file_info)

        # Zip::File.open_buffer(data.body) do |zip_file|
        #   file_list = {}
        #
        #   zip_file.each do |entry|
        #     line_count = 0
        #
        #     read_zis(entry.get_input_stream) do |message|
        #       break unless message
        #       # puts '[[' + filename + ' |> ' + entry.name + ']]: ' + message
        #
        #       line_count = line_count + 1
        #       yield message if block_given?
        #     end
        #
        #     file_list[entry.name.to_sym] = {
        #         size: entry.size,
        #         compressed_size: entry.compressed_size,
        #         time: entry.time,
        #         crc: entry.crc,
        #         line_count: line_count,
        #     }
        #   end
        #
        #   file_info = {
        #       key: prefix + filename,
        #       content_length: data.content_length,
        #       last_modified: data.last_modified,
        #       etag: data.etag,
        #       finish_time: Time.now.to_i,
        #       file_list: file_list,
        #   }
        #
        #   finish_file(prefix_key, filename, file_info)
        # end
      end

      def read_zis(zis)
        begin
          loop do
            line = zis.readline
            yield line.chomp if block_given?
            break if zis.eof
          end
        rescue EOFError
        end
      end

      def finish_file(prefix_key, filename, file_info)
        finished_keys = @cursors_data[:finished_keys]
        finished_files = finished_keys[prefix_key.to_sym]

        if finished_files.nil?
          finished_files = {}
          finished_keys[prefix_key.to_sym] = finished_files
        end

        finished_files[filename.to_sym] = file_info

        next_prefix_keys

        time_elapsed("aws_s3_loader::finish_file(#{prefix_key}, #{filename}) - #{file_info}") do
          save_cursors
        end

        # puts '[[ FINISHED ]]: ' + file_info[:key]
      end

      # def node_match?(filename)
      #   return true if @node_count < 2
      #   num = 0
      #   filename.bytes.each { |c| num ^= c }
      #   (num % @node_count == @node_index)
      # end
    end

  end
end
