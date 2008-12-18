$:.unshift File.expand_path(File.dirname(File.expand_path(__FILE__)))
require 'base_mq'
require 'amqp/synchronous_client'

# A synchronous implementation of the Ruby-tied AMQP library.
class SyncMQ < BaseMQ
  def initialize(init_connection = nil)
    @connection = init_connection || AMQP::SynchronousClient.new
    @channel = connection.add_channel(self)

    send(Protocol::Channel::Open.new)
    connection.process_frames until ready?
  end
  attr_reader :channel
  
  def send(*args)
    (@_send_mutex ||= Mutex.new).synchronize do
      args.each do |data|
        data.ticket = @ticket if @ticket and data.respond_to? :ticket=
        log :sending, data
        connection.send data, :channel => @channel
      end
    end
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

  def self.error(msg = nil, &blk)
    if blk
      @error_callback = blk
    else
      @error_callback.call(msg) if @error_callback and msg
    end
  end

  def callback
    yield self
  end
end
