module Foundation
  module Internal
    class << self; attr_accessor :env, :error_handler; end

    module InheritForHashAndArray
      def symbolize_keys
        symbolize_keys { |k| k.to_sym rescue k }
      end
      def symbolize_keys(&b)
        return inject({}){|r,(k,v)| r[k.to_sym] = (v.symbolize_keys(&b) rescue v); r} if is_a? Hash
        return inject([]){|r,v    | r          << (v.symbolize_keys(&b) rescue v); r} if is_a? Array
        return self
      end
    end

  module InheritForColorize
      def black;          "\e[30m#{self}\e[0m" end
      def red;            "\e[31m#{self}\e[0m" end
      def green;          "\e[32m#{self}\e[0m" end
      def yellow;         "\e[33m#{self}\e[0m" end
      def blue;           "\e[34m#{self}\e[0m" end
      def magenta;        "\e[35m#{self}\e[0m" end
      def cyan;           "\e[36m#{self}\e[0m" end
      def gray;           "\e[37m#{self}\e[0m" end

      def bg_black;       "\e[40m#{self}\e[0m" end
      def bg_red;         "\e[41m#{self}\e[0m" end
      def bg_green;       "\e[42m#{self}\e[0m" end
      def bg_yellow;      "\e[43m#{self}\e[0m" end
      def bg_blue;        "\e[44m#{self}\e[0m" end
      def bg_magenta;     "\e[45m#{self}\e[0m" end
      def bg_cyan;        "\e[46m#{self}\e[0m" end
      def bg_gray;        "\e[47m#{self}\e[0m" end

      def bold;           "\e[1m#{self}\e[22m" end
      def italic;         "\e[3m#{self}\e[23m" end
      def underline;      "\e[4m#{self}\e[24m" end
      def blink;          "\e[5m#{self}\e[25m" end
      def reverse_color;  "\e[7m#{self}\e[27m" end
    end

    module InheritForString
      include InheritForColorize
    end

    module InheritForException
      def p(bt = 1)
        puts s.red
        puts self.backtrace unless bt == 0
      end
      def s()
        "#{self.class}: #{self.message}"
      end
    end

  end

  Hash.class_eval { include Internal::InheritForHashAndArray }
  Array.class_eval { include Internal::InheritForHashAndArray }
  String.class_eval { include Internal::InheritForString }
  Exception.class_eval { include Internal::InheritForException }

end

module Kernel

  def this_method
    if self.class == Class
      "#{self.to_s}.#{caller[0][/`(.*)'/, 1]}"
    else
      "#{self.class}##{caller[0][/`(.*)'/, 1]}"
    end
  end

  def calling_method(level = 1)
    caller[level] =~ /`([^']*)'/ and $1
  end

  def calling_from
    caller[1].split(/:\d+/,2).first
  end

  def catch_error(ex, opts = nil)
    Foundation::Internal::error_handler.catch(ex, opts) if Foundation::Internal::error_handler
  end

end
