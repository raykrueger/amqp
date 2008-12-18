require 'test_helper'
require 'sync_mq'

class SynchronousBasicOperationTests < Test::Unit::TestCase
  def setup
  end

  def teardown
  end

  def test_enqueue_and_dequeue
    #AMQP.logging = true
    SyncMQ.new.queue('test').publish('foo')

    #Timeout::timeout(2) do
    #  SyncMQ.new.queue('test').subscribe do |message|
    #   @received = message
    #  end
    #end
    #assert_equal 'foo', @received
  end

  #queue_test(:enqueue_before_dequeue) do
  #  MQ.new.queue('test').publish('foo')

  #  MQ.new.queue('test').subscribe do |message|
  #    assert_equal 'foo', message
  #    queue_test_done
  #  end
  #end

  #queue_test(:dequeue_before_enqueue) do
  #  MQ.new.queue('test').subscribe do |message|
  #    assert_equal 'foo', message
  #    queue_test_done
  #  end
  #  MQ.new.queue('test').publish('foo')
  #end

  #queue_test(:messaging_processing_fails) do
  #  MQ.new.queue('test').publish('message')

  #  when_published_to('test') do
  #    chan = MQ.new
  #    chan.queue('test').pop(:ack => true) do |h, message|
  #      # Closing the channel will cause the unacked message to
  #      # be redelivered.
  #      chan.close 

  #      # Setup a new pop request to regrab the message and
  #      # successfully process it.
  #      MQ.new.queue('test').pop(:ack => true) do |header, message|
  #        header.ack
  #        queue_test_done
  #      end
  #    end
  #  end
  #end
end
