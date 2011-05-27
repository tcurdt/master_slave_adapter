module ActiveRecord

  module ConnectionAdapters

    class MasterSlaveAdapter

      class Clock
        include Comparable
        attr_reader :file, :position
        def initialize(file, position)
          @file, @position = file, position.to_i
        end
        def <=>(other)
          @file == other.file ? @position <=> other.position : @file <=> other.file
        end
        def to_s
          "#{@file}@#{@position}"
        end
        def self.zero
          @zero ||= Clock.new('', 0)
        end
     end

      SELECT_METHODS = [ :select_all, :select_one, :select_rows, :select_value, :select_values ]

      include ActiveSupport::Callbacks
      define_callbacks :checkout, :checkin

      checkout :test_connections

      attr_accessor :connections
      attr_accessor :master_config
      attr_accessor :slave_configs
      attr_accessor :disable_connection_test


      delegate :select_all, :select_one, :select_rows, :select_value, :select_values, :to => :select_connection

      def initialize(config)
        if config[:master].blank?
          raise "There is no :master config in the database configuration provided -> #{config.inspect} "
        end

        @slave_connections = []

        config = config.symbolize_keys

        common_config = config.reject do |k|
          [ :master, :slaves, :eager_load_connections, :disable_connection_test, :master_slave_adapter ].include? k
        end
        common_config[:adapter] = config[:master_slave_adapter]

        self.master_config = common_config.merge(config[:master]||{})
        self.slave_configs = config[:slaves].inject([]) do |memo, (slave_id,slave_config)|
          memo << common_config.merge(slave_config).merge({ :id => slave_id })
        end

        self.disable_connection_test = config[:disable_connection_test] == 'true'

        self.connections = []
        if config[:eager_load_connections] == 'true'
          connect_to_master
          for i in 0...self.slave_configs.length
            connect_to_slave(i)
          end
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

      def commit_db_transaction()
        on_write do
          self.master_connection.commit_db_transaction()
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

      def slave_connection(i)
        connect_to_slave(i)
      end

      def connections
        ([ @master_connection ] + @slave_connections).compact
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
          Thread.current[:master_slave_select_connection] = [ :master ] + (Thread.current[:master_slave_select_connection]||[])
          result = yield
          Thread.current[:master_slave_select_connection] = Thread.current[:master_slave_select_connection].drop(1)
          result
        end

        def with_slave
          Thread.current[:master_slave_select_connection] = [ :slave ] + (Thread.current[:master_slave_select_connection]||[])
          result = yield
          Thread.current[:master_slave_select_connection] = Thread.current[:master_slave_select_connection].drop(1)
          result
        end


        def with_consistency(clock)
          raise ArgumentError, "consistency cannot be nil" if !clock

          Thread.current[:master_slave_select_connection] = [ nil ] + (Thread.current[:master_slave_select_connection]||[])
          Thread.current[:master_slave_clock] = [ clock || Clock::zero ] + (Thread.current[:master_slave_clock]||[])

          yield
          result = Thread.current[:master_slave_clock][0]

          Thread.current[:master_slave_clock] = Thread.current[:master_slave_clock].drop(1)
          Thread.current[:master_slave_select_connection] = Thread.current[:master_slave_select_connection].drop(1)
          result
        end

        def reset!
          Thread.current[:master_slave_select_connection] = nil
          Thread.current[:master_slave_clock] = nil
        end

        def master_forced?
          Thread.current[:master_slave_enabled] == true
        end

        def master_forced=(state)
          Thread.current[:master_slave_enabled] = state ? true : nil
        end

      end

      private

      def update_clock
        # update the clock, if there was problem keep using the old one
        Thread.current[:master_slave_clock][0] = master_clock || Thread.current[:master_slave_clock][0]
        # it's a write so from now on we use the master connection
        # as replication is not likely to be that fast
        Thread.current[:master_slave_select_connection][0] = :master
      end

      def on_write
        result = yield
        if !MasterSlaveAdapter.master_forced? && @master_connection.open_transactions == 0
          update_clock
        end
        result
      end

      def connection_for_clock(required_clock, i)
        if required_clock
          # check the slave for it's replication state
          if clock = slave_clock(i)
            if clock >= required_clock
              # slave is safe to use
              :slave
            else
              # slave is not there yet
              :master
            end
          else
            # not getting slave status, better turn to master
            # maybe this should be logged or raised?
            :master
          end
        else
          # no required clock so slave is good enough
          :slave
        end
      end

      def pick_slave(slaves)
        slaves_count = slaves.length
        slaves_count > 1 ? rand(slaves.length) : 0
      end

      def select_connection
        connection_stack = Thread.current[:master_slave_select_connection] ||= []
        clock_stack = Thread.current[:master_slave_clock] ||= []

        # pick the right connection
        if MasterSlaveAdapter.master_forced? || @master_connection.open_transactions > 0
          connection_stack[0] = :master
        end

        i = pick_slave(self.slave_configs)

        connection_stack[0] ||= connection_for_clock(clock_stack[0], i)

        # return the current connection
        if connection_stack[0] == :slave
          slave_connection(i)
        else
          master_connection
        end
      end

      def master_clock
        if status = connect_to_master.select_one("SHOW MASTER STATUS")
          Clock.new(status['File'], status['Position'])
        end
      end

      def slave_clock(i)
        if status = connect_to_slave(i).select_one("SHOW SLAVE STATUS")
          Clock.new(status['Master_Log_File'], status['Exec_Master_Log_Pos'])
        end
      end

      def connect_to_master
        @master_connection ||= ActiveRecord::Base.send("#{self.master_config[:adapter]}_connection", self.master_config)
      end

      def connect_to_slave(i)
        @slave_connections[i] ||= ActiveRecord::Base.send("#{self.slave_configs[i][:adapter]}_connection", self.slave_configs[i])
      end

    end

  end

end