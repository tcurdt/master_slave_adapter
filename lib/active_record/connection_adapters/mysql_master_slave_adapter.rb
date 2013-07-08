require 'active_record/connection_adapters/master_slave_adapter'
require 'active_record/connection_adapters/master_slave_adapter/shared_mysql_adapter_behavior'
require 'active_record/connection_adapters/mysql_adapter'
require 'mysql'

module ActiveRecord
  class Base
    def self.mysql_master_slave_connection(config)
      ConnectionAdapters::MysqlMasterSlaveAdapter.new(config, logger)
    end
  end

  module ConnectionAdapters
    class MysqlMasterSlaveAdapter < AbstractAdapter
      include MasterSlaveAdapter
      include SharedMysqlAdapterBehavior

    private

      CONNECTION_ERRORS = [
        Mysql::Error::CR_CONNECTION_ERROR,  # query: not connected
        Mysql::Error::CR_CONN_HOST_ERROR,   # Can't connect to MySQL server on '%s' (%d)
        Mysql::Error::CR_SERVER_GONE_ERROR, # MySQL server has gone away
        Mysql::Error::CR_SERVER_LOST,       # Lost connection to MySQL server during query
      ]

      def connection_error?(exception)
        case exception
        when ActiveRecord::StatementInvalid
          CONNECTION_ERRORS.include?(current_connection.raw_connection.errno)
        when Mysql::Error
          CONNECTION_ERRORS.include?(exception.errno)
        else
          false
        end
      end

    end
  end
end
