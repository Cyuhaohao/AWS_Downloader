module GreatEye

  class ILoader
    def initialize(options = nil)
      raise NotImplementedError, "Implement this method in a child class"
    end

    def open()
      raise NotImplementedError, "Implement this method in a child class"
    end

    def read()
      raise NotImplementedError, "Implement this method in a child class"
    end

    def close()
      raise NotImplementedError, "Implement this method in a child class"
    end
  end

end
