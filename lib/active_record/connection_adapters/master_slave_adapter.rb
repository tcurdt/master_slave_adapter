# require 'active_record/connection_adapters/abstract/database_statements'
# require 'active_record/connection_adapters/abstract/schema_statements'
# 
# require 'master_slave_adapter/adapter'
# require 'master_slave_adapter/instance_methods_generation'

require 'master_slave_adapter/active_record_base_extensions'

require 'master_slave_adapter/connections/master_connection'
require 'master_slave_adapter/connections/slave_connection'
require 'master_slave_adapter/connections/consistent_connection'

module ActiveRecord
  module ConnectionAdapters
    class MasterSlaveAdapter

      def initialize( config )
        if config[:master].blank?
          raise "There is no :master config in the database configuration provided -> #{config.inspect} "
        end
        # self.slave_config = config.symbolize_keys
        # self.master_config = self.slave_config.delete(:master).symbolize_keys
        # self.slave_config[:adapter] = self.slave_config.delete(:master_slave_adapter)
        # self.master_config[:adapter] ||= self.slave_config[:adapter]
        # self.disable_connection_test = self.slave_config.delete( :disable_connection_test ) == 'true'

        @master = nil
        @slaves = nil

        @connection = ::MasterSlaveAdapter::Connections::MasterConnection.new(@master, @slaves)
        @master_forced = false
        @with_stack = []
      end

      def master_forced?
        @master_forced
      end
      def master_forced=(forced)
        @master_forced = forced
      end

      def with_stack
        @with_stack
      end


      # delegation

      def connection
        @connection
      end

      def method_missing(name, *args, &block)
        # puts "missing: #{name}"
        self.connection.send(name.to_sym, *args, &block)
      end


      # delegation to "with" wrappers

      def with_master(&block)
        return connection.with(&block) if self.master_forced?
        with_stack.push connection
        connection = ::MasterSlaveAdapter::Connections::MasterConnection.new(@master, @slaves)
        result = connection.with(&block)
        connection = with_stack.pop
        result
      end

      def with_slave(&block)
        return connection.with(&block) if self.master_forced?
        with_stack.push connection
        connection = ::MasterSlaveAdapter::Connections::SlaveConnection.new(@master, @slaves)
        result = connection.with(&block)
        connection = with_stack.pop
        result
      end

      def with_consistency(clock, &block)
        return connection.with(&block) if self.master_forced?
        with_stack.push connection
        connection = ::MasterSlaveAdapter::Connections::ConsistentConnection.new(@master, @slaves)
        result = connection.with(clock, &block)
        connection = with_stack.pop
        result
      end

      class << self

        def reset!
        end

        # def method_missing(name, *args, &block)
        #   self.connection.send(name.to_sym, *args, &block)
        # end
      end

      private

    end
  end
end
