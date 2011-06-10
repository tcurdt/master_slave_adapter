require 'master_slave_adapter/connections/adapter_connection'

module MasterSlaveAdapter
  module Connections
    class ConsistentConnection < AdapterConnection

      class Clock
        include Comparable
        attr_reader :file, :position
        def initialize(file, position)
          raise ArgumentError, "file and postion may not be nil" if file.nil? || position.nil?
          @file, @position = file, position.to_i
        end
        def <=>(other)
          @file == other.file ? @position <=> other.position : @file <=> other.file
        end
        def to_s
          "#{@file}@#{@position}"
        end
        def self.zero
          @zero ||= Clock.new('', 0)
        end
      end

      def initialize(master, slaves)
      end

      def with(clock)
        yield
        clock
      end

      private
      # def update_clock
      #   # puts " update clock"
      #   # update the clock, if there was problem keep using the old one
      #   self.current_clock[0] = master_clock || self.current_clock[0]
      #   # it's a write so from now on we use the master connection
      #   # as replication is not likely to be that fast
      #   self.current_connection[0] = :master
      # end
      # 
      # def on_write
      #   result = yield
      #   if !MasterSlaveAdapter.master_forced? && @master_connection.open_transactions == 0
      #     update_clock
      #   end
      #   result
      # end
      # 
      # def connection_for_clock(required_clock)
      #   if required_clock
      #     # check the slave for it's replication state
      #     if clock = slave_clock
      #       if clock >= required_clock
      #         # slave is safe to use
      #         :slave
      #       else
      #         # slave is not there yet
      #         :master
      #       end
      #     else
      #       # not getting slave status, better turn to master
      #       # maybe this should be logged or raised?
      #       :master
      #     end
      #   else
      #     # no required clock so slave is good enough
      #     :slave
      #   end
      # end
      # 
      # def select_connection
      #   connection_stack = self.current_connection
      #   clock_stack = self.current_clock
      # 
      #   # pick the right connection
      #   if MasterSlaveAdapter.master_forced? || @master_connection.open_transactions > 0
      #     connection_stack[0] = :master
      #   end
      # 
      #   connection_stack[0] ||= connection_for_clock(clock_stack[0])
      # 
      #   # return the current connection
      #   if connection_stack[0] == :slave
      #     slave_connection
      #   else
      #     master_connection
      #   end
      # end
      # 
      # def master_clock
      #   # puts " master clock"
      #   connection = connect_to_master
      #   if status = connection.uncached { connection.select_one("SHOW MASTER STATUS") }
      #     Clock.new(status['File'], status['Position'])
      #   end
      # end
      # 
      # def slave_clock
      #   # puts " slave clock"
      #   connection = connect_to_slave
      #   if status = connection.uncached { connection.select_one("SHOW SLAVE STATUS") }
      #     Clock.new(status['Relay_Master_Log_File'], status['Exec_Master_Log_Pos'])
      #   end
      # end

    end
  end
end