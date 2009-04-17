require 'mq'

class SyncMQ < MQ
  @@thread = nil

  class << self
    def start_reactor
      unless EM.reactor_running?
        @@thread = Thread.new { EM.run } if @@thread.nil? || !@@thread.alive?
      end
    end
  end
  
  def initialize(connection = nil)
    self.class.start_reactor
    super(connection)
    spin_until(true) { self.connected }
  end

  def close
    super
    spin_until(false) { self.connected }
  end

  private
    def spin_until(desired)
      until yield() == desired
        sleep(0.1)
      end 
    end
end
