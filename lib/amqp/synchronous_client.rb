module AMQP
  class SynchronousClient
    include AMQP::BasicClient

    def initialize
      @settings = AMQP.settings
      @buf = Buffer.new
      connect
    end

    def connect
      socket.write HEADER
      socket.write [1, 1, VERSION_MAJOR, VERSION_MINOR].pack('C4')

      process_frames until connection_ready?
    end

    def close
      if connection_ready?
        send Protocol::Connection::Close.new(:reply_code => 200,
                                             :reply_text => 'Goodbye',
                                             :class_id => 0,
                                             :method_id => 0)

        process_frames while connection_ready?
      end
    end

    # Pulls any pending data off the socket and processes it.
    def process_frames
      wait_on_socket
      while frame = get_frame_nonblock
        process_frame(frame)
      end
    end

    private
      def socket
        @socket ||= TCPSocket.open(AMQP.settings[:host], AMQP.settings[:port])
      end

      def send_data(data)
        socket.write data
      end

      def wait_on_socket
        res = select([socket])
        if res
          @buf << res[0][0].read_nonblock(1024)
        end
      end

      # Pulls the next frame off the buffer if there are any
      def get_frame_nonblock
        frame = Frame.parse(@buf)
        log 'receive', frame if frame
        frame
      end
  end
end
