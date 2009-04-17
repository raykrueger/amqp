gem 'amqp', '0.7.0'
require 'sync_mq'

SyncMQ.ensure_reactor_running

AMQP.logging = true

mq = SyncMQ.new
mq.queue('foo').publish('bar')
mq.close
