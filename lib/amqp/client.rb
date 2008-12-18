require 'amqp/frame'

module AMQP
  class Error < StandardError; end

  module BasicClient
    def channels
      @channels ||= {}
    end

    def add_channel mq
      (@_channel_mutex ||= Mutex.new).synchronize do
        channels[ key = (channels.keys.max || 0) + 1 ] = mq
        key
      end
    end
  
    def process_frame frame
      if mq = channels[frame.channel]
        mq.process_frame(frame)
        return
      end
      
      case frame
      when Frame::Method
        case method = frame.payload
        when Protocol::Connection::Start
          send Protocol::Connection::StartOk.new({:platform => 'Ruby/EventMachine',
                                                  :product => 'AMQP',
                                                  :information => 'http://github.com/tmm1/amqp',
                                                  :version => VERSION},
                                                 'AMQPLAIN',
                                                 {:LOGIN => @settings[:user],
                                                  :PASSWORD => @settings[:pass]},
                                                 'en_US')

        when Protocol::Connection::Tune
          send Protocol::Connection::TuneOk.new(:channel_max => 0,
                                                :frame_max => 131072,
                                                :heartbeat => 0)

          send Protocol::Connection::Open.new(:virtual_host => @settings[:vhost],
                                              :capabilities => '',
                                              :insist => @settings[:insist])

        when Protocol::Connection::OpenOk
          @connection_ready = true
          connection_did_become_ready if respond_to?(:connection_did_become_ready)

        when Protocol::Connection::Close
          @connected = false
          log "#{method.reply_text} in #{Protocol.classes[method.class_id].methods[method.method_id]}"

        when Protocol::Connection::CloseOk
          @connection_ready = false
          @on_disconnect.call if @on_disconnect
        end
      end
    end

    def connected?
      @connected
    end
  
    # True if the AMQP protocl connection is established
    def connection_ready?
      @connection_ready
    end

    def send data, opts = {}
      channel = opts[:channel] ||= 0
      data = data.to_frame(channel) unless data.is_a? Frame
      data.channel = channel

      log 'send', data
      send_data data.to_s
    end

    private
      def log *args
        return unless @settings[:logging] or AMQP.logging
        require 'pp'
        pp args
        puts
      end
  end

  def self.client
    @client ||= BasicClient
  end
  
  def self.client= mod
    mod.__send__ :include, AMQP
    @client = mod
  end

  module Client
    include EM::Deferrable

    attr_accessor :reconnect

    def initialize opts = {}
      @settings = opts
      extend AMQP.client

      @on_disconnect = proc do
        @connected = false
        log "Could not connect to server #{opts[:host]}:#{opts[:port]}" 
      end

      timeout @settings[:timeout] if @settings[:timeout]
      errback do
        @connected = false
        if @settings.key?(:reconnect)
          EM.add_timer(5) { Client.connect @settings, &(@settings[:reconnect]) }
        else
          @on_disconnect.call
        end
      end
    end

    def connection_completed
      log 'connected'
      @connected = true

      if @settings.key?(:reconnect)
        @settings[:reconnect].call self
        @on_disconnect = nil
      else
        @on_disconnect = proc do
          @connected = false
          log 'Disconnected from server'
        end
      end

      @buf = Buffer.new
      send_data HEADER
      send_data [1, 1, VERSION_MAJOR, VERSION_MINOR].pack('C4')
    end

    def unbind
      log 'disconnected'
      @connected = false

      EM.next_tick do
        if @settings.key?(:reconnect)
          EM.add_timer(5) { Client.connect @settings, &(@settings[:reconnect]) }
        else
          @on_disconnect.call
        end
      end
    end

    def connection_did_become_ready
      succeed(self)
    end

    def receive_data data
      # log 'receive_data', data
      @buf << data

      while frame = Frame.parse(@buf)
        log 'receive', frame
        process_frame frame
      end
    end

    def process_frame frame
      # this is a stub meant to be
      # replaced by the module passed into initialize
    end
  
    def send data, opts = {}
      channel = opts[:channel] ||= 0
      data = data.to_frame(channel) unless data.is_a? Frame
      data.channel = channel

      log 'send', data
      send_data data.to_s
    end

    # def send_data data
    #   log 'send_data', data
    #   super
    # end

    def close &on_disconnect
      @on_disconnect = on_disconnect if on_disconnect
      @settings.delete(:reconnect)

      callback{ |c|
        if c.channels.any?
          c.channels.each do |ch, mq|
            mq.close
          end
        else
          send Protocol::Connection::Close.new(:reply_code => 200,
                                               :reply_text => 'Goodbye',
                                               :class_id => 0,
                                               :method_id => 0)
        end
      }
    end
  
    def self.connect opts = {}, &blk
      opts = AMQP.settings.merge(opts)
      opts[:reconnect] = blk if block_given?

      EM.connect opts[:host], opts[:port], self, opts
    end
  
  end
end
