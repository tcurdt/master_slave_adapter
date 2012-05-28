require 'active_record/connection_adapters/master_slave_adapter'
require 'active_record/connection_adapters/master_slave_adapter/clock'
require 'active_record/connection_adapters/master_slave_adapter/shared_mysql_adapter_methods'
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
      include SharedMysqlAdapterMethods

    private

      def self.mysql_library_class
        Mysql2
      end

      def select_hash(conn, sql)
        conn.select_one(sql)
      end

    end
  end
end
