module ActiveRecord

  module ConnectionAdapters

    class MasterSlaveAdapter

      class Clock
        include Comparable
        def initialize(file, position)
          @file, @position = file, position
        end
        def <=>(other)
          @file == other.file ? @position <=> other.position : @file <=> other.file
        end
        def inspect
          "#{@file}@#{position}"
        end
        def self.ZERO
          @zero |= Clock.new('', 0)
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


      delegate :select_all, :select_one, :select_rows, :select_value, :select_values, :to => :select_connection

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
        on_write do
          self.master_connection.insert(sql, *args)
        end
      end

      def update(sql, *args)
        on_write do
          self.master_connection.update(sql, *args)
        end
      end

      def delete(sql, *args)
        on_write do
          self.master_connection.delete(sql, *args)
        end
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


        def with_consistency(clock)
          # clock is only motonic increasing
          Thread.current[:clock] = [ Thread.current[:clock] || Clock::ZERO, clock ].max
          # explicitly ask for a evaluation to select a connection
          Thread.current[:select_connection] = nil
          yield
          # clear reference
          Thread.current[:select_connection] = nil
          # return the latest clock, might or might not have been changed
          Thread.current[:clock]
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

      def on_write
        result = yield

        # update the clock
        if status = self.slave_connection.select_one("SHOW MASTER STATUS")
          # update clock to the lastest status
          Thread.current[:clock] = Clock.new(status[:master_log_file], status[:log_pos])
        else
          # means we are in master only setup so a clock is not required
        end

        # it's a write so from now on we use the master connection
        # as replication is not likely to be that fast
        Thread.current[:select_connection] = self.master_connection
      end

      def pick
        if required_clock = Thread.current[:clock]
          # we are in a with_consistency block
          if status = self.slave_connection.select_one("SHOW SLAVE STATUS")
            slave_clock = Clock.new(status[:relay_master_log_file], status[:relay_log_pos])
            if slave_clock >= required_clock
              # slave is safe to use
              self.slave_connection
            else
              # slave is not there yet
              self.master_connection
            end
          else
            # not getting slave status, better turn to master
            self.master_connection
          end
        else
          # no with_consistency, normal behaviour for select is to go to slave
          self.slave_connection
        end
      end

      def select_connection
        if Thread.current[:select_connection] == nil
          # has not picked a connection yet
          Thread.current[:select_connection] = pick
        else
          # we stick with the current connection
          Thread.current[:select_connection]
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