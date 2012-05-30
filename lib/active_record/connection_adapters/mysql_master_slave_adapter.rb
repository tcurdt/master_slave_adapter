require 'active_record/connection_adapters/master_slave_adapter'
require 'active_record/connection_adapters/master_slave_adapter/clock'
require 'active_record/connection_adapters/master_slave_adapter/shared_mysql_adapter_methods'
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
      include SharedMysqlAdapterMethods

    private

      if MysqlAdapter.instance_methods.map(&:to_sym).include?(:exec_without_stmt)
        # The MysqlAdapter in ActiveRecord > v3.1 uses prepared statements which
        # don't return any results for queries like "SHOW MASTER/SLAVE STATUS",
        # so we have to use normal queries here.
        def select_hash(conn, sql)
          conn.exec_without_stmt(sql).first
        end
      else
        def select_hash(conn, sql)
          conn.select_one(sql)
        end
      end

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
