$:.unshift File.dirname(__FILE__) + '/../../lib'
require 'sync_mq'


def log *args
  p args
end

#AMQP.logging = true

class HashTable < Hash
  def get key
    log 'HashTable', :get, key
    self[key]
  end
  
  def set key, value
    log 'HashTable', :set, key => value
    self[key] = value
  end

  def keys
    log 'HashTable', :keys
    super
  end
end

SyncMQ.start_reactor
server_mq = SyncMQ.new
server = server_mq.rpc('hash table node', HashTable.new)

client_mq = SyncMQ.new
client = client_mq.rpc('hash table node')
client.set(:now, time = Time.now)
res = client.get(:now)
log 'client', :now => res, :eql? => res == time

client.set(:one, 1)
log 'client', :keys => client.keys

server_mq.close
client_mq.close

__END__

["HashTable", :set, {:now=>Thu Jul 17 21:04:53 -0700 2008}]
["HashTable", :get, :now]
["HashTable", :set, {:one=>1}]
["HashTable", :keys]
["client", {:eql?=>true, :now=>Thu Jul 17 21:04:53 -0700 2008}]
["client", {:keys=>[:one, :now]}]
