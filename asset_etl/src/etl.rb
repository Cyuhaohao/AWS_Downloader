#!/usr/bin/env ruby

STDOUT.sync = true # let the output direct without buffer

def help_exit
  puts <<HELP
Usage: etl.rb CONFIG_FILE
HELP
  # exit 1
end

cfg = ARGV.shift
return help_exit if cfg.empty?

require_relative 'etl/etl_app'
Puzzle::AssetETL::ETLApp.new(cfg).run
