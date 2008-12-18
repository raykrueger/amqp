$:.unshift File.expand_path(File.dirname(File.expand_path(__FILE__)))
require 'amqp'
require 'base_mq'

class MQ < BaseMQ
  include EM::Deferrable

  def initialize connection = nil
    raise 'MQ can only be used from within EM.run{}' unless EM.reactor_running?

    @connection = connection || AMQP.start

    conn.callback{ |c|
      @channel = c.add_channel(self)
      send Protocol::Channel::Open.new
    }
  end
  attr_reader :channel

  def send(*args)
    conn.callback{ |c|
      (@_send_mutex ||= Mutex.new).synchronize do
        args.each do |data|
          data.ticket = @ticket if @ticket and data.respond_to? :ticket=
          log :sending, data
          c.send data, :channel => @channel
        end
      end
    }
  end

  def close
    if @deferred_status == :succeeded
      send Protocol::Channel::Close.new(:reply_code => 200,
                                        :reply_text => 'bye',
                                        :method_id => 0,
                                        :class_id => 0)
    else
      @closing = true
    end
  end

  # error callback

  def self.error msg = nil, &blk
    if blk
      @error_callback = blk
    else
      @error_callback.call(msg) if @error_callback and msg
    end
  end

  def channel_did_become_ready
    succeed(self)
  end
end
