module GreatEye

  class IParser
    def initialize(options = nil)
      raise NotImplementedError, "Implement this method in a child class"
    end

    def init()
      raise NotImplementedError, "Implement this method in a child class"
    end

    def destroy()
      raise NotImplementedError, "Implement this method in a child class"
    end

    def parse(data)
      raise NotImplementedError, "Implement this method in a child class"
    end
  end

end
