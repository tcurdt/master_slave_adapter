ActiveRecord::Base.class_eval do

  def reload_with_master( options = nil )
    ActiveRecord::ConnectionAdapters::MasterSlaveAdapter.with_master do
      reload_without_master( options )
    end
  end

  alias_method_chain :reload, :master

  class << self

    # Call this method to force a block of code to use the master connection
    #
    # ActiveRecord::Base.with_master do
    #   User.count( :conditions => { :login => 'testuser' } )
    # end
    def with_master(&block)
      if connection.respond_to? :with_master
        connection.with_master(&block)
      else
        raise "no with_master"
      end
    end

    # Call this method to force a block of code to use the slave connection
    #
    # ActiveRecord::Base.with_slave do
    #   User.count( :conditions => { :login => 'testuser' } )
    # end
    def with_slave(&block)
      if connection.respond_to? :with_slave
        connection.with_slave(&block)
      else
        raise "no with_slave"
      end
    end

    # Call this method to force a certain binlog position.
    # Going to the slave if it's already at the position otherwise
    # falling back to master.
    #
    # consistency = ActiveRecord::Base.with_consistency(consistency) do
    #   User.count( :conditions => { :login => 'testuser' } )
    # end
    def with_consistency(clock, &block)
      if connection.respond_to? :with_consistency
        connection.with_consistency(clock, &block)
      else
        raise "no with_consistency"
      end
    end

    def transaction_with_master(*args, &block)
      if connection.respond_to? :transaction
        connection.transaction do
          transaction_without_master(*args, &block)
        end
      else
        transaction_without_master(*args, &block)
      end
    end
    alias_method_chain :transaction, :master


    def master_slave_connection( config )
      config = config.symbolize_keys
      raise "You must provide a configuration for the master database - #{config.inspect}" if config[:master].blank?
      raise "You must provide a 'master_slave_adapter' value at your database config file" if config[:master_slave_adapter].blank?

      unless self.respond_to?( "#{config[:master_slave_adapter]}_connection" )

        begin
          require 'rubygems'
          gem "activerecord-#{config[:master_slave_adapter]}-adapter"
          require "active_record/connection_adapters/#{config[:master_slave_adapter]}_adapter"
        rescue LoadError
          begin
            require "active_record/connection_adapters/#{config[:master_slave_adapter]}_adapter"
          rescue LoadError
            raise "Please install the #{config[:master_slave_adapter]} adapter: `gem install activerecord-#{config[:master_slave_adapter]}-adapter` (#{$!})"
          end
        end

      end

      ActiveRecord::ConnectionAdapters::MasterSlaveAdapter.new(config)
    end

    def columns_with_master
      ActiveRecord::ConnectionAdapters::MasterSlaveAdapter.with_master do
        columns_without_master
      end
    end
    alias_method_chain :columns, :master

  end
end
