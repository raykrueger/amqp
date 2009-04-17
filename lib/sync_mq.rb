require 'mq'

class SyncMQ < MQ
  Dir[File.expand_path(File.join(File.dirname(__FILE__), 'sync_mq', '*.rb'))].each do |file|
    require file
  end

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

  def rpc(name, obj = nil)
    blocking_rpcs[name] ||= RPC.new(self, name, obj)
  end

  def close
    super
    spin_until(false) { self.connected }
  end

  def blocking_rpcs
    @blocking_rpcs ||= {}
  end

  private
    def spin_until(desired)
      until yield() == desired
        sleep(0.1)
      end 
    end
end
