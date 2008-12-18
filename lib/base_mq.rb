require 'amqp'

# A base class that implements the majority of the logic for the 
# Ruby-tized version of the AMQP protocol.
class BaseMQ
  include AMQP

  Dir[File.join(File.dirname(__FILE__), 'mq/*.rb')].each do |file|
    require file
  end

  class << self
    @logging = false
    attr_accessor :logging
  end

  class Error < StandardError; end

  def process_frame(frame)
    log :received, frame

    case frame
    when Frame::Header
      @header = frame.payload
      @body = ''

    when Frame::Body
      @body << frame.payload
      if @body.length >= @header.size
        @header.properties.update(@method.arguments)
        @consumer.receive @header, @body if @consumer
        @body = @header = @consumer = @method = nil
      end

    when Frame::Method
      case method = frame.payload
      when Protocol::Channel::OpenOk
        send Protocol::Access::Request.new(:realm => '/data',
                                           :read => true,
                                           :write => true,
                                           :active => true,
                                           :passive => true)

      when Protocol::Access::RequestOk
        @ticket = method.ticket
        callback{
          send Protocol::Channel::Close.new(:reply_code => 200,
                                            :reply_text => 'bye',
                                            :method_id => 0,
                                            :class_id => 0)
        } if @closing
        @channel_ready = true
        channel_did_become_ready

      when Protocol::Basic::CancelOk
        if @consumer = consumers[ method.consumer_tag ]
          @consumer.cancelled
        else
          MQ.error "Basic.CancelOk for invalid consumer tag: #{method.consumer_tag}"
        end

      when Protocol::Queue::DeclareOk
        queues[ method.queue ].recieve_status method

      when Protocol::Basic::Deliver, Protocol::Basic::GetOk
        @method = method
        @header = nil
        @body = ''

        if method.is_a? Protocol::Basic::GetOk
          @consumer = get_queue{|q| q.shift }
          MQ.error "No pending Basic.GetOk requests" unless @consumer
        else
          @consumer = consumers[ method.consumer_tag ]
          MQ.error "Basic.Deliver for invalid consumer tag: #{method.consumer_tag}" unless @consumer
        end

      when Protocol::Basic::GetEmpty
        @consumer = get_queue{|q| q.shift }
        @consumer.receive nil, nil

      when Protocol::Channel::Close
        raise Error, "#{method.reply_text} in #{Protocol.classes[method.class_id].methods[method.method_id]} on #{@channel}"

      when Protocol::Channel::CloseOk
        @closing = false
        conn.callback{ |c|
          c.channels.delete @channel
          c.close if c.channels.empty?
        }
      end
    end
  end

  # A callback that will be executed when the channel has just become
  # ready for more commands. Subclasses can use this to manage their
  # state.
  def channel_did_become_ready
  end

  # Returns true if this channel is ready for further requests.
  def channel_ready?
    @channel_ready
  end
  alias :ready? :channel_ready?

  def queue name, opts = {}
    queues[name] ||= Queue.new(self, name, opts)
  end

  def rpc name, obj = nil
    rpcs[name] ||= RPC.new(self, name, obj)
  end

  # keep track of proxy objects
  #
  %w[ direct topic fanout ].each do |type|
    class_eval %[
      def #{type} name = 'amq.#{type}', opts = {}
        exchanges[name] ||= Exchange.new(self, :#{type}, name, opts)
      end
    ]
  end
  
  def exchanges
    @exchanges ||= {}
  end

  def queues
    @queues ||= {}
  end

  def get_queue
    if block_given?
      (@get_queue_mutex ||= Mutex.new).synchronize{
        yield( @get_queue ||= [] )
      }
    end
  end

  def rpcs
    @rcps ||= {}
  end

  # queue objects keyed on their consumer tags

  def consumers
    @consumers ||= {}
  end

  private
    def log *args
      return unless self.class.logging
      pp args
      puts
    end

    attr_reader :connection
    alias :conn :connection
end
