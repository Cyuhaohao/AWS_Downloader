module Foundation
  module Util

    module ExternalMethods
      # extend self
      # module_function :symbolize_keys

      MAX_TIMESTAMP_IN_SEC = (10 ** 11 - 1).freeze

      def symbolize_keys(obj)
        return obj.inject({}){|r,(k,v)| r[k.to_sym] = symbolize_keys(v); r} if obj.is_a? Hash
        return obj.inject([]){|r,v    | r          << symbolize_keys(v); r} if obj.is_a? Array
        return obj
      end

      def normalize_option(v)
        return v unless v.is_a?(String)
        return v if v.length == 0
        cf, cl = v[0], v[-1]
        return v if cf != cl
        if (cf == "'" or cf == '"')
          return '' if v.length == 1
          return v[1..-2]
        end
        return v
      end

      def timeout(sec)
        return yield(sec) if sec == nil or sec.zero?
        Thread.start {
          sleep sec
          return yield(sec)
        }
      end

      # help to check if the timestamp is correct
      def normalize_timestamp_in_ms(tm)
        return tm * 1000 if tm <= MAX_TIMESTAMP_IN_SEC

        tm
      end

      def timestamp_in_ms_to_string(timestamp, format = '%Y-%m-%d %H:%M:%S %z')
        timestamp_in_ms_to_time(timestamp).strftime(format)
      end

      def timestamp_in_ms_to_time(timestamp)
        Time.at(timestamp.to_f / 1000).utc
      end

      def timestamp_to_string(timestamp, format = '%Y-%m-%d %H:%M:%S %z')
        timestamp_to_time(timestamp).strftime(format)
      end

      def timestamp_to_time(timestamp)
        Time.at(timestamp).utc
      end
    end

    extend ExternalMethods

  end
end
