require 'test_helper'

require 'amqp/synchronous_client'

class SynchronousLowLevelTest < Test::Unit::TestCase
  def test_connecting_to_queue
    #AMQP.logging = true
    client = AMQP::SynchronousClient.new
    assert client.connection_ready?
  end

  def test_closing_connections
    client = AMQP::SynchronousClient.new
    assert client.connection_ready?

    client.close
    assert !client.connection_ready?
  end
end
