# convenience wrapper (read: HACK) for thread-local MQ object
class BaseMQ
  class << self
    def default
      # XXX clear this when connection is closed
      Thread.current[self.class.name] ||= self.new
    end

    def method_missing meth, *args, &blk
      self.default.__send__(meth, *args, &blk)
    end
  end
end

# unique identifier
class BaseMQ
  class << self
    def id
      Thread.current["#{self.class.name}_id"] ||= "#{`hostname`.strip}-#{Process.pid}-#{Thread.current.object_id}"
    end
  end
end
