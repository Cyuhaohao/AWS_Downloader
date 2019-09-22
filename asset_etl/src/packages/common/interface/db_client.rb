module Puzzle

  module Packages

    class IDBClient
      def initialize(options = nil)
        raise NotImplementedError, "Implement this method in a child class"
      end

      def connect()
        raise NotImplementedError, "Implement this method in a child class"
      end

      def close()
        raise NotImplementedError, "Implement this method in a child class"
      end

      def execute(sql)
        raise NotImplementedError, "Implement this method in a child class"
      end

      def prepare(sql)
        raise NotImplementedError, "Implement this method in a child class"
      end

      def stmt_execute(stmt, args)
        raise NotImplementedError, "Implement this method in a child class"
      end
    end

  end

end
