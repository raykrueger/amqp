require 'test/unit'

require 'rubygems'

gem 'amqp'
require 'mq'

gem 'activesupport'
require 'activesupport'

class Test::Unit::TestCase
  def verbose(message)
    puts '***************************************************************************'
    puts message
    puts '***************************************************************************'
  end

  class << self
    def queue_test(name, options = {}, &block)
      define_method("test_#{name}") do
        self.instance_exec do
          EM.run do
            AMQP.start
            AMQP.logging = false

            EM.add_timer(options[:timeout] || 5) do
              # In case the test never completes, time it out.
              AMQP.stop { EM.stop }
              @timed_out = true
            end

            self.instance_exec(&block)
          end

          fail("Test timed out.") if @timed_out
        end
      end
    end
  end

  # Stops the queue_test, which requires shutting down the 
  # EventMachine reactor cleanly. Tests created using the
  # queue_test declaration should use this method to 
  # signify that they completed successfully. Otherwise,
  # they will eventually time out.
  def queue_test_done
    AMQP.stop{ EM.stop }
  end

  def when_published_to(queue_name)
    publish_spin_lock = EM::PeriodicTimer.new(0.1) do
      MQ.new.queue(queue_name).status do |messages,|
        @published = messages > 0
      end
      if @published
        publish_spin_lock.cancel
        yield
      end
    end
  end
end
