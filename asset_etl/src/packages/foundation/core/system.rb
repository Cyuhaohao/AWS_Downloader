module Foundation
  module System

    class IErrorHandler
      def catch(ex, opts) end
    end

    def loaded
      require_relative '../internal/kernel'
      # require_relative '../internal/**'

      env('dev')
    end

    def env(env = nil)
      Internal::env = env if env
      Internal::env
    end

    def error_handler(handler = nil)
      if handler and handler.is_a?(IErrorHandler)
        Internal::error_handler = handler
      end
      Internal::error_handler
    end

    module_function :loaded
    module_function :env
    module_function :error_handler

  end
end
