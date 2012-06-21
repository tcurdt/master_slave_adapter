$: << File.expand_path(File.join(File.dirname( __FILE__ ), '..', '..', 'lib'))

require 'rspec'
require 'active_record/connection_adapters/mysql2_master_slave_adapter'

module ActiveRecord
  class Base
    cattr_accessor :master_mock, :slave_mock
    def self.mysql2_connection(config)
      config[:database] == 'slave' ? slave_mock : master_mock
    end
  end
end

describe ActiveRecord::ConnectionAdapters::Mysql2MasterSlaveAdapter do
  let(:database_setup) do
    {
      :adapter => 'master_slave',
      :username => 'root',
      :database => 'slave',
      :connection_adapter => 'mysql2',
      :master => { :username => 'root', :database => 'master' },
      :slaves => [{ :database => 'slave' }],
    }
  end

  let(:mocked_methods) do
    {
      :reconnect! => true,
      :disconnect! => true,
      :active? => true,
    }
  end

  let!(:master_connection) do
    mock(
      'master connection',
      mocked_methods.merge(:open_transactions => 0)
    ).tap do |conn|
      conn.stub!(:uncached).and_yield
      ActiveRecord::Base.master_mock = conn
    end
  end

  let!(:slave_connection) do
    mock('slave connection', mocked_methods).tap do |conn|
      conn.stub!(:uncached).and_yield
      ActiveRecord::Base.slave_mock = conn
    end
  end

  def adapter_connection
    ActiveRecord::Base.connection
  end

  SelectMethods = [ :select_all, :select_one, :select_rows, :select_value, :select_values ] unless defined?(SelectMethods)
  Clock = ActiveRecord::ConnectionAdapters::MasterSlaveAdapter::Clock unless defined?(Clock)

  before do
    ActiveRecord::Base.establish_connection(database_setup)
  end

  after do
    ActiveRecord::Base.connection_handler.clear_all_connections!
  end

  describe 'consistency' do
    def zero
      Clock.zero
    end

    def master_position(pos)
      Clock.new('', pos)
    end

    def select_method
      :select_one
    end

    def should_report_clock(pos, connection, log_file, log_pos, sql)
      pos = Array(pos)
      values = pos.map { |p| { log_file => '', log_pos => p } }

      connection.
        should_receive(select_method).exactly(pos.length).times.
        with(sql).
        and_return(*values)
    end

    def slave_should_report_clock(pos)
      should_report_clock(pos, slave_connection, 'Relay_Master_Log_File', 'Exec_Master_Log_Pos', 'SHOW SLAVE STATUS')
    end

    def master_should_report_clock(pos)
      should_report_clock(pos, master_connection, 'File', 'Position', 'SHOW MASTER STATUS')
    end

    SelectMethods.each do |method|
      it "should send the method '#{method}' to the slave if nil is given" do
        slave_should_report_clock(0)
        slave_connection.should_receive(method).with('testing').and_return(true)
        new_clock = ActiveRecord::Base.with_consistency(nil) do
          adapter_connection.send(method, 'testing')
        end
        new_clock.should be_a(Clock)
        new_clock.should equal(zero)
      end

      it "should send the method '#{method}' to the slave if clock.zero is given" do
        slave_should_report_clock(0)
        slave_connection.should_receive(method).with('testing').and_return(true)
        old_clock = zero
        new_clock = ActiveRecord::Base.with_consistency(old_clock) do
          adapter_connection.send(method, 'testing')
        end
        new_clock.should be_a(Clock)
        new_clock.should equal(old_clock)
      end

      it "should send the method '#{method}' to the master if slave hasn't cought up to required clock yet" do
        slave_should_report_clock(0)
        master_connection.should_receive(method).with('testing').and_return(true)
        old_clock = master_position(1)
        new_clock = ActiveRecord::Base.with_consistency(old_clock) do
          adapter_connection.send(method, 'testing' )
        end
        new_clock.should be_a(Clock)
        new_clock.should equal(old_clock)
      end

      it "should send the method '#{method}' to the master connection if there are open transactions" do
        master_connection.stub!(:open_transactions).and_return(1)
        master_connection.should_receive(method).with('testing').and_return(true)
        old_clock = zero
        new_clock = ActiveRecord::Base.with_consistency(old_clock) do
          adapter_connection.send(method, 'testing')
        end
        new_clock.should be_a(Clock)
        new_clock.should equal(zero)
      end

      it "should send the method '#{method}' to the master after a write operation" do
        slave_should_report_clock(0)
        master_should_report_clock(2)
        slave_connection.should_receive(method).with('testing').and_return(true)
        master_connection.should_receive(:update).with('testing').and_return(true)
        master_connection.should_receive(method).with('testing').and_return(true)
        old_clock = zero
        new_clock = ActiveRecord::Base.with_consistency(old_clock) do
          adapter_connection.send(method, 'testing')   # slave
          adapter_connection.send(:update, 'testing') # master
          adapter_connection.send(method, 'testing')   # master
        end
        new_clock.should be_a(Clock)
        new_clock.should > old_clock
      end
    end

    it "should update the clock after a transaction" do
      slave_should_report_clock(0)
      master_should_report_clock([0, 1, 1])

      slave_connection.
        should_receive(:select_all).exactly(1).times.with('testing').
        and_return(true)

      master_connection.
        should_receive(:update).exactly(3).times.with('testing').
        and_return(true)
      master_connection.
        should_receive(:select_all).exactly(5).times.with('testing').
        and_return(true)
      %w(begin_db_transaction
         commit_db_transaction
         increment_open_transactions
         decrement_open_transactions
         outside_transaction?).each do |txstmt|
        master_connection.should_receive(txstmt).exactly(1).times
      end

      master_connection.
        should_receive('open_transactions').exactly(13).times.
        and_return(
          # adapter: with_consistency, select_all, update, select_all
          0, 0, 0, 0,
          # connection: transaction
          0,
          # adapter: select_all, update, select_all, commit_db_transaction
          1, 1, 1, 0,
          # connection: transaction (ensure)
          0,
          # adapter: select_all, update, select_all
          0, 0, 0
         )

      old_clock = zero
      new_clock = ActiveRecord::Base.with_consistency(old_clock) do
        adapter_connection.send(:select_all, 'testing') # slave  s=0 m=0
        adapter_connection.send(:update, 'testing')     # master s=0 m=1
        adapter_connection.send(:select_all, 'testing') # master s=0 m=1

        ActiveRecord::Base.transaction do
          adapter_connection.send(:select_all, 'testing') # master s=0 m=1
          adapter_connection.send(:update, 'testing')     # master s=0 m=1
          adapter_connection.send(:select_all, 'testing') # master s=0 m=1
        end

        adapter_connection.send(:select_all, 'testing') # master s=0 m=2
        adapter_connection.send(:update, 'testing')     # master s=0 m=3
        adapter_connection.send(:select_all, 'testing') # master s=0 m=3
      end

      new_clock.should > old_clock
    end

    context "with nested with_consistency" do
      it "should return the same clock if not writing and no lag" do
        slave_should_report_clock(0)
        slave_connection.
          should_receive(:select_one).exactly(3).times.with('testing').
          and_return(true)

        old_clock = zero
        new_clock = ActiveRecord::Base.with_consistency(old_clock) do
          adapter_connection.send(:select_one, 'testing')
          ActiveRecord::Base.with_consistency(old_clock) do
            adapter_connection.send(:select_one, 'testing')
          end
          adapter_connection.send(:select_one, 'testing')
        end
        new_clock.should equal(old_clock)
      end

      it "requesting a newer clock should return a new clock" do
        adapter_connection.
          should_receive('slave_consistent?').exactly(2).times.
          and_return(true, false)
        slave_connection.
          should_receive(:select_all).exactly(2).times.with('testing').
          and_return(true)
        master_connection.
          should_receive(:select_all).exactly(1).times.with('testing').
          and_return(true)

        start_clock = zero
        inner_clock = zero
        outer_clock = ActiveRecord::Base.with_consistency(start_clock) do
          adapter_connection.send(:select_all, 'testing') # slave
          inner_clock = ActiveRecord::Base.with_consistency(master_position(1)) do
            adapter_connection.send(:select_all, 'testing') # master
          end
          adapter_connection.send(:select_all, 'testing') # slave
        end

        start_clock.should equal(outer_clock)
        inner_clock.should > start_clock
      end
    end

    it "should do the right thing when nested inside with_master" do
      slave_should_report_clock(0)
      slave_connection.should_receive(:select_all).exactly(1).times.with('testing').and_return(true)
      master_connection.should_receive(:select_all).exactly(2).times.with('testing').and_return(true)
      ActiveRecord::Base.with_master do
        adapter_connection.send(:select_all, 'testing') # master
        ActiveRecord::Base.with_consistency(zero) do
          adapter_connection.send(:select_all, 'testing') # slave
        end
        adapter_connection.send(:select_all, 'testing') # master
      end
    end

    it "should do the right thing when nested inside with_slave" do
      slave_should_report_clock(0)
      slave_connection.should_receive(:select_all).exactly(3).times.with('testing').and_return(true)
      ActiveRecord::Base.with_slave do
        adapter_connection.send(:select_all, 'testing') # slave
        ActiveRecord::Base.with_consistency(zero) do
          adapter_connection.send(:select_all, 'testing') # slave
        end
        adapter_connection.send(:select_all, 'testing') # slave
      end
    end

    it "should do the right thing when wrapping with_master" do
      slave_should_report_clock(0)
      slave_connection.should_receive(:select_all).exactly(2).times.with('testing').and_return(true)
      master_connection.should_receive(:select_all).exactly(1).times.with('testing').and_return(true)
      ActiveRecord::Base.with_consistency(zero) do
        adapter_connection.send(:select_all, 'testing') # slave
        ActiveRecord::Base.with_master do
          adapter_connection.send(:select_all, 'testing') # master
        end
        adapter_connection.send(:select_all, 'testing') # slave
      end
    end

    it "should do the right thing when wrapping with_slave" do
      slave_should_report_clock(0)
      slave_connection.should_receive(:select_all).exactly(1).times.with('testing').and_return(true)
      master_connection.should_receive(:select_all).exactly(2).times.with('testing').and_return(true)
      ActiveRecord::Base.with_consistency(master_position(1)) do
        adapter_connection.send(:select_all, 'testing') # master
        ActiveRecord::Base.with_slave do
          adapter_connection.send(:select_all, 'testing') # slave
        end
        adapter_connection.send(:select_all, 'testing') # master
      end
    end

    it "should accept clock as string" do
      slave_should_report_clock(0)
      slave_connection.should_receive(:select_all).with('testing')

      ActiveRecord::Base.with_consistency("@0") do
        adapter_connection.send(:select_all, 'testing')
      end
    end
  end # /with_consistency

  describe "connection error detection" do
    {
      2002 => "query: not connected",
      2003 => "Can't connect to MySQL server on 'localhost' (3306)",
      2006 => "MySQL server has gone away",
      2013 => "Lost connection to MySQL server during query",
    }.each do |errno, description|
      it "raises MasterUnavailable for '#{description}' during query execution" do
        master_connection.stub_chain(:raw_connection, :errno).and_return(errno)
        master_connection.should_receive(:insert).and_raise(ActiveRecord::StatementInvalid.new("Mysql2::Error: #{description}: INSERT 42"))

        expect do
          adapter_connection.insert("INSERT 42")
        end.to raise_error(ActiveRecord::MasterUnavailable)
      end

      it "doesn't raise anything for '#{description}' during connection" do
        error = Mysql2::Error.new(description)
        error.stub(:errno).and_return(errno)
        ActiveRecord::Base.should_receive(:mysql2_connection).twice do |config|
          if config[:name] == :master
            raise error
          else
            slave_connection
          end
        end

        expect do
          ActiveRecord::Base.connection_handler.clear_all_connections!
          ActiveRecord::Base.connection
        end.to_not raise_error
      end
    end

    it "raises MasterUnavailable for 'closed MySQL connection' during query execution" do
      master_connection.should_receive(:insert).and_raise(ActiveRecord::StatementInvalid.new("Mysql2::Error: closed MySQL connection: INSERT 42"))

      expect do
        adapter_connection.insert("INSERT 42")
      end.to raise_error(ActiveRecord::MasterUnavailable)
    end

    it "raises StatementInvalid for other errors" do
      master_connection.should_receive(:insert).and_raise(ActiveRecord::StatementInvalid.new("Mysql2::Error: Query execution was interrupted: INSERT 42"))

      expect do
        adapter_connection.insert("INSERT 42")
      end.to raise_error(ActiveRecord::StatementInvalid)
    end
  end
end
