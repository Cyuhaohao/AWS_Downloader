module GreatEye

  class IStorer
    def initialize(options = nil)
      raise NotImplementedError, "Implement this method in a child class"
    end

    def open()
      raise NotImplementedError, "Implement this method in a child class"
    end

    def close()
      raise NotImplementedError, "Implement this method in a child class"
    end

    def write(data)
      raise NotImplementedError, "Implement this method in a child class"
    end
  end

end
