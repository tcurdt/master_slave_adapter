require 'active_record/connection_adapters/master_slave_adapter'
require 'active_record/connection_adapters/master_slave_adapter/clock'
require 'active_record/connection_adapters/master_slave_adapter/shared_mysql_adapter_behavior'
require 'active_record/connection_adapters/mysql2_adapter'
require 'mysql2'

module ActiveRecord
  class Base
    def self.mysql2_master_slave_connection(config)
      ConnectionAdapters::Mysql2MasterSlaveAdapter.new(config, logger)
    end
  end

  module ConnectionAdapters
    class Mysql2MasterSlaveAdapter < AbstractAdapter
      include MasterSlaveAdapter
      include SharedMysqlAdapterBehavior

    private

      def select_hash(conn, sql)
        conn.select_one(sql)
      end

      CONNECTION_ERRORS = {
        2002 => "query: not connected",                         # CR_CONNECTION_ERROR
        2003 => "Can't connect to MySQL server on",             # CR_CONN_HOST_ERROR
        2006 => "MySQL server has gone away",                   # CR_SERVER_GONE_ERROR
        2013 => "Lost connection to MySQL server during query", # CR_SERVER_LOST
        -1   => "closed MySQL connection",                      # defined by Mysql2
      }

      def connection_error?(exception)
        case exception
        when ActiveRecord::StatementInvalid
          CONNECTION_ERRORS.values.any? do |description|
            exception.message.start_with?("Mysql2::Error: #{description}")
          end
        when Mysql2::Error
          CONNECTION_ERRORS.keys.include?(exception.errno)
        else
          false
        end
      end

    end
  end
end
