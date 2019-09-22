require 'pathname'
require 'optparse'
require 'yaml'

module Foundation

  class IAppEntry

    def initialize
      @result = {}
    end

    def set_result(key, value)
      @result[key] = value
    end

    def run_result
      @result
    end

    def run(opts) end
    def terminal(err) end
  end

  class App

    CONFIG_ROOT = '../config'

    @@entry_class = nil
    @@entry_classes = {}

    def self.Entry(entry_class)
      unless entry_class and entry_class.ancestors.include?(IAppEntry)
        raise TypeError, 'Invalid entry type.'
      end
      @@entry_class = entry_class
    end

    def initialize(root_path, options = {})
      @root_path = root_path
      @options = options

      @app_entry = nil
      @args_data = nil
      @config_data = nil
      @game_config = nil

      @trap_signal = false

      yield self if block_given?

      init_args
      init_entry
      init_config
    end

    def run(&block)
      return unless @app_entry

      if @trap_signal
        Signal.trap('INT') { @app_entry.terminal('INT'); exit }
        Signal.trap('TERM') { @app_entry.terminal('TERM'); exit }
      end

      @app_entry.run(
        root_path: @root_path,
        args: @args_data,
        config: @config_data,
        game_config: @game_config,
        &block
      )

      @app_entry.run_result
    end

    private

    def init_args()
      @args_data = {}

      c = @options[:command]
      return unless c.is_a?(Hash)

      parser = OptionParser.new do |p|
        p.banner = c[:message] if c[:message]

        if c[:on].is_a?(Array)
          args = @args_data
          p.separator 'Specific options:'
          c[:on].each do |a|
            continue if a.empty?
            k = a.shift
            p.on(*a) { |v| args[k] = Util::normalize_option(v) }
          end
        end

        if c[:help]
          p.separator 'Common options:'
          p.on_tail(*c[:help]) do
            puts p
            exit
          end
        end
      end

      begin
        parser.parse!(ARGV)
      rescue Exception => ex
        ex.p 0
        puts parser
        exit 1
      end
    end

    def init_entry()
      entry = @options[:entry]
      return unless entry

      if (Pathname.new entry).relative?
        entry = File.expand_path(@root_path + '/' + entry)
      end

      #load entry_class
      unless @@entry_classes.key?(entry)
        require entry

        unless @@entry_class
          raise 'Entry not found.'
        end

        @@entry_classes[entry] = @@entry_class

        @@entry_class = nil
      end

      @app_entry = @@entry_classes[entry].new
    end

    def init_config
      load_etl_config
      load_game_config

      @config_data[:user_data] = @options[:user_data]
    end

    def load_etl_config
      config_file_path = File.join(@root_path, CONFIG_ROOT, @options[:config])
      @config_data = YAML.load_file(config_file_path).symbolize_keys
    end

    def load_game_config
      game_config_file_path = File.join(@root_path, CONFIG_ROOT, @options[:game_config])
      @game_config = YAML.load_file(game_config_file_path).symbolize_keys

      raise 'game env is not set' unless @game_config[:game_env]
      System.env(@game_config[:game_env]) if @game_config[:game_env]
    end
  end

end
