require 'active_record/connection_adapters/master_slave_adapter/clock'

module ActiveRecord
  module ConnectionAdapters
    module MasterSlaveAdapter
      module SharedMysqlAdapterBehavior
        def with_consistency(clock)
          clock =
            case clock
            when Clock  then clock
            when String then Clock.parse(clock)
            when nil    then Clock.zero
            end

          super(clock)
        end

        def master_clock
          conn = master_connection
          if status = conn.uncached { conn.select_one("SHOW MASTER STATUS") }
            Clock.new(status['File'], status['Position'])
          else
            Clock.infinity
          end
        rescue MasterUnavailable
          Clock.zero
        rescue ActiveRecordError
          Clock.infinity
        end

        def slave_clock(conn)
          if status = conn.uncached { conn.select_one("SHOW SLAVE STATUS") }
            Clock.new(status['Relay_Master_Log_File'], status['Exec_Master_Log_Pos'])
          else
            Clock.zero
          end
        rescue ActiveRecordError
          Clock.zero
        end
      end
    end
  end
end
