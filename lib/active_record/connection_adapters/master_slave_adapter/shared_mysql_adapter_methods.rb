module ActiveRecord
  module ConnectionAdapters
    module MasterSlaveAdapter
      module SharedMysqlAdapterMethods
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
          if status = conn.uncached { select_hash(conn, "SHOW MASTER STATUS") }
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
          if status = conn.uncached { select_hash(conn, "SHOW SLAVE STATUS") }
            Clock.new(status['Relay_Master_Log_File'], status['Exec_Master_Log_Pos'])
          else
            Clock.zero
          end
        rescue ActiveRecordError
          Clock.zero
        end

      private

        CONNECTION_ERRORS = [
          2002, # CR_CONNECTION_ERROR  - query: not connected
          2003, # CR_CONN_HOST_ERROR   - Can't connect to MySQL server on '%s' (%d)
          2006, # CR_SERVER_GONE_ERROR - MySQL server has gone away
          2013, # CR_SERVER_LOST       - Lost connection to MySQL server during query
        ]

        def connection_error?(exception)
          case exception
          when ActiveRecord::StatementInvalid
            CONNECTION_ERRORS.include?(current_connection.raw_connection.errno)
          when self.class.mysql_library_class::Error
            CONNECTION_ERRORS.include?(exception.errno)
          else
            false
          end
        end
      end
    end
  end
end