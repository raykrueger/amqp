require 'monitor'

class SyncMQ
  class RPC
    def initialize(mq, queue, obj = nil)
      @async_rpc = MQ::RPC.new(mq, queue, obj)
    end

    ResultHolder = Struct.new(:object)
    def method_missing(meth, *args, &block)
      result_holder = ResultHolder.new
      result_holder.extend(MonitorMixin)
      cond_var = result_holder.new_cond

      @async_rpc.send(meth, *args) do |resp|
        result_holder.synchronize do
          result_holder.object = resp
          cond_var.signal
        end
      end

      result_holder.synchronize { cond_var.wait(60) || raise('RPC called timed out') }
      result_holder.object
    end
  end
end
