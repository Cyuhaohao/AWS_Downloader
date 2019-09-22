
module Foundation

  # init
  path = __dir__
  # path = File.dirname(__FILE__)
  $:.unshift(path) unless $:.include?(path)

  # module
  autoload :Util, 'util/util'
  autoload :System, 'core/system'

  # class
  # autoload :App, 'core/app'

  # loaded
  System::loaded

end
