class MQ
  class Header
    include AMQP

    def initialize(mq, header_obj)
      @mq = mq
      @header_obj = header_obj
    end

    def properties
      @header_obj.properties
    end

    # Acknowledges the receipt of this message with the server.
    def ack
      @mq.callback do
        @mq.send Protocol::Basic::Ack.new({ :delivery_tag => @header_obj.properties[:delivery_tag] })
      end
    end
  end
end
