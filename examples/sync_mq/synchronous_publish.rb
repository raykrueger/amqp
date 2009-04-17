$:.unshift File.dirname(__FILE__) + '/../../lib'
require 'sync_mq'

# Must call first to interact with the raw AMQP layer
SyncMQ.start_reactor

AMQP.logging = true

mq = SyncMQ.new
mq.queue('foo').publish('bar')
mq.close
