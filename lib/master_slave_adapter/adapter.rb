module ActiveRecord

  module ConnectionAdapters

    class MasterSlaveAdapter

      class Consistency
        include Comparable
        def initialize(file, position)
          @file, @position = file, position
        end
        def <=>(other)
          @file <=> other.file && @position <=> other.position
        end
        def inspect
          "#{@file}@#{position}"
        end
     end

      SELECT_METHODS = [ :select_all, :select_one, :select_rows, :select_value, :select_values ]

      include ActiveSupport::Callbacks
      define_callbacks :checkout, :checkin

      checkout :test_connections

      attr_accessor :connections
      attr_accessor :master_config
      attr_accessor :slave_config
      attr_accessor :disable_connection_test


      delegate :select_all, :select_one, :select_rows, :select_value, :select_values, :to => :pick

      def initialize( config )
        if config[:master].blank?
          raise "There is no :master config in the database configuration provided -> #{config.inspect} "
        end
        self.slave_config = config.symbolize_keys
        self.master_config = self.slave_config.delete(:master).symbolize_keys
        self.slave_config[:adapter] = self.slave_config.delete(:master_slave_adapter)
        self.master_config[ :adapter ] ||= self.slave_config[:adapter]
        self.disable_connection_test = self.slave_config.delete( :disable_connection_test ) == 'true'
        self.connections = []
        if self.slave_config.delete( :eager_load_connections ) == 'true'
          connect_to_master
          connect_to_slave
        end
      end

      def insert(sql, *args)
        result = self.master_connection.insert(sql, *args)
        update_consistency
        result
      end

      def update(sql, *args)
        result = self.master_connection.update(sql, *args)
        update_consistency
        result
      end

      def delete(sql, *args)
        result = self.master_connection.delete(sql, *args)
        update_consistency
        result
      end

      def slave_connection
        if ActiveRecord::ConnectionAdapters::MasterSlaveAdapter.master_enabled?
          master_connection
        elsif @master_connection && @master_connection.open_transactions > 0
          master_connection
        else
          connect_to_slave
        end
      end

      def reconnect!
        @active = true
        self.connections.each { |c| c.reconnect! }
      end

      def disconnect!
        @active = false
        self.connections.each { |c| c.disconnect! }
      end

      def reset!
        self.connections.each { |c| c.reset! }
      end

      def method_missing( name, *args, &block )
        self.master_connection.send( name.to_sym, *args, &block )
      end

      def master_connection
        connect_to_master
      end

      def connections
        [ @master_connection, @slave_connection ].compact
      end

      def test_connections
        return if self.disable_connection_test
        self.connections.each do |c|
          begin
            c.select_value( 'SELECT 1', 'test select' )
          rescue
            c.reconnect!
          end
        end
      end

      class << self

        def with_master
          if master_enabled?
            yield
          else
            enable_master
            begin
              yield
            ensure
              disable_master
            end
          end
        end

        def with_slave
          if master_enabled?
            disable_master
            begin
              yield
            ensure
              enable_master
            end
          else
            yield
          end
        end


        def with_consistency(consistency)
          Thread.current[:consistency] = consistency
          Thread.current[:try_slave] = true
          yield
          Thread.current[:consistency]
        end

        def master_enabled?
          Thread.current[ :master_slave_enabled ]
        end

        def enable_master
          Thread.current[ :master_slave_enabled ] = true
        end

        def disable_master
          Thread.current[ :master_slave_enabled ] = nil
        end

      end

      private

      def pick
        if Thread.current[:try_slave] && status = self.slave_connection.select_one("SHOW SLAVE STATUS")
          cur_consistency = Consistency.new(status[:relay_master_log_file], status[:relay_log_pos])
          req_consistency = Thread.current[:consistency]
          if !req_consistency || cur_consistency >= req_consistency
            self.slave_connection
          else
            Thread.current[:try_slave] = false
            self.master_connection
          end
        else
          self.master_connection
        end
      end

      def update_consistency
        if status = self.slave_connection.select_one("SHOW MASTER STATUS")
          Thread.current[:consistency] = Consistency.new(status[:master_log_file], status[:log_pos])
        end
      end

      def connect_to_master
        @master_connection ||= ActiveRecord::Base.send( "#{self.master_config[:adapter]}_connection", self.master_config )
      end

      def connect_to_slave
        @slave_connection ||= ActiveRecord::Base.send( "#{self.slave_config[:adapter]}_connection", self.slave_config)
      end

    end

  end

end