$: << File.expand_path(File.join(File.dirname(__FILE__), '..', 'lib'))

require 'rspec'
require 'active_record/connection_adapters/master_slave_adapter'

ActiveRecord::Base.logger =
  Logger.new($stdout).tap { |l| l.level = Logger::DEBUG }

module ActiveRecord
  class Base
    cattr_accessor :master_mock, :slave_mock
    def self.test_connection(config)
      config[:database] == 'slave' ? slave_mock : master_mock
    end

    def self.test_master_slave_connection(config)
      TestMasterSlaveAdapter.new(config, logger)
    end
  end

  module ConnectionAdapters
    class TestMasterSlaveAdapter < MasterSlaveAdapter::Base
      def master_clock
      end

      def slave_clock(connection)
      end

      def connection_error?(exception)
        true
      end
    end
  end
end

describe ActiveRecord::ConnectionAdapters::MasterSlaveAdapter do
  let(:default_database_setup) do
    {
      :adapter => 'master_slave',
      :username => 'root',
      :database => 'slave',
      :connection_adapter => 'test',
      :master => { :username => 'root', :database => 'master' },
      :slaves => [{ :database => 'slave' }],
    }
  end

  let(:database_setup) { default_database_setup }

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

  SchemaStatements = ActiveRecord::ConnectionAdapters::SchemaStatements.public_instance_methods.map(&:to_sym)
  SelectMethods = [ :select_all, :select_one, :select_rows, :select_value, :select_values ]
  Clock = ActiveRecord::ConnectionAdapters::MasterSlaveAdapter::Clock

  before do
    ActiveRecord::Base.establish_connection(database_setup)
  end

  after do
    ActiveRecord::Base.connection_handler.clear_all_connections!
  end

  describe 'common configuration' do
    it "should call 'columns' on master" do
      master_connection.should_receive(:columns)
      adapter_connection.columns
    end

    SelectMethods.each do |method|
      it "should send the method '#{method}' to the slave connection" do
        master_connection.stub!( :open_transactions ).and_return( 0 )
        slave_connection.should_receive( method ).with('testing').and_return( true )
        adapter_connection.send( method, 'testing' )
      end

      it "should send the method '#{method}' to the master connection if with_master was specified" do
        master_connection.should_receive( method ).with('testing').and_return( true )
        ActiveRecord::Base.with_master do
          adapter_connection.send( method, 'testing' )
        end
      end

      it "should send the method '#{method}' to the slave connection if with_slave was specified" do
        slave_connection.should_receive( method ).with('testing').and_return( true )
        ActiveRecord::Base.with_slave do
          adapter_connection.send( method, 'testing' )
        end
      end

      it "should send the method '#{method}' to the master connection if there are open transactions" do
        master_connection.stub!( :open_transactions ).and_return( 1 )
        master_connection.should_receive( method ).with('testing').and_return( true )
        ActiveRecord::Base.with_master do
          adapter_connection.send( method, 'testing' )
        end
      end

      it "should send the method '#{method}' to the master connection if there are open transactions, even in with_slave" do
        master_connection.stub!( :open_transactions ).and_return( 1 )
        master_connection.should_receive( method ).with('testing').and_return( true )
        ActiveRecord::Base.with_slave do
          adapter_connection.send( method, 'testing' )
        end
      end
    end # /SelectMethods.each

    SchemaStatements.each do |method|
      it "should send the method '#{method}' from ActiveRecord::ConnectionAdapters::SchemaStatements to the master"  do
        master_connection.should_receive( method ).and_return( true )
        adapter_connection.send( method )
      end
    end

    (SchemaStatements - SelectMethods).each do |method|
      it "should send the method '#{method}' from ActiveRecord::ConnectionAdapters::DatabaseStatements to the master"  do
        master_connection.should_receive( method ).and_return( true )
        adapter_connection.send( method )
      end
    end

    it "should call #visitor on master connection" do
      master_connection.should_receive(:visitor)
      adapter_connection.visitor
    end

    it 'should be a master slave connection' do
      adapter_connection.class.should == ActiveRecord::ConnectionAdapters::TestMasterSlaveAdapter
    end

    it 'should have a master connection' do
      adapter_connection.master_connection.should == master_connection
    end

    it 'should have a slave connection' do
      adapter_connection.slave_connection!.should == slave_connection
    end
  end

  describe "connection testing" do
    before do
      master_connection.unstub(:active?)
      slave_connection.unstub(:active?)
    end

    context "disabled" do
      let(:database_setup) do
        default_database_setup.merge(:disable_connection_test => 'true')
      end

      it "should not perform the testing" do
        master_connection.should_not_receive(:active?)
        slave_connection.should_not_receive(:active?)

        adapter_connection.active?.should == true
      end
    end

    context "enabled" do
      it "should perform the testing" do
        # twice == one during connection + one on explicit #active? call
        master_connection.should_receive(:active?).twice.and_return(true)
        slave_connection.should_receive(:active?).twice.and_return(true)

        adapter_connection.active?.should == true
      end
    end
  end

  describe 'with connection eager loading enabled' do
    it 'should eager load the connections' do
      adapter_connection.connections.should include(master_connection, slave_connection)
    end
  end

  describe "transaction callbacks" do
    def run_tx
      adapter_connection.
        should_receive('master_clock').
        and_return(Clock.new('', 1))
      %w(begin_db_transaction
         commit_db_transaction
         increment_open_transactions
         decrement_open_transactions
         outside_transaction?).each do |txstmt|
        master_connection.
          should_receive(txstmt).exactly(1).times
      end
      master_connection.
        should_receive('open_transactions').exactly(4).times.
        and_return(0, 1, 0, 0)

      master_connection.
        should_receive('update').with('testing').
        and_return(true)

      ActiveRecord::Base.transaction do
        adapter_connection.send('update', 'testing')
      end
    end

    def fail_tx
      %w(begin_db_transaction
         rollback_db_transaction
         increment_open_transactions
         decrement_open_transactions).each do |txstmt|
        master_connection.
          should_receive(txstmt).exactly(1).times
      end
      master_connection.
        should_receive('outside_transaction?').exactly(2).times
      master_connection.
        should_receive('open_transactions').exactly(3).times.
        and_return(0, 1, 0)
      master_connection.
        should_receive('update').with('testing').
        and_return(true)

      ActiveRecord::Base.transaction do
        adapter_connection.send('update', 'testing')
        raise "rollback"
      end
    rescue
      nil
    end

    context "on commit" do
      it "on_commit callback should be called" do
        x = false
        adapter_connection.on_commit { x = true }
        lambda { run_tx }.should change { x }.to(true)
      end

      it "on_rollback callback should not be called" do
        x = false
        adapter_connection.on_rollback { x = true }
        lambda { run_tx }.should_not change { x }
      end
    end

    context "on rollback" do
      it "on_commit callback should not be called" do
        x = false
        adapter_connection.on_commit { x = true }
        lambda { fail_tx }.should_not change { x }
      end

      it "on_rollback callback should be called" do
        x = false
        adapter_connection.on_rollback { x = true }
        lambda { fail_tx }.should change { x }.to(true)
      end
    end
  end

  describe "query cache" do
    describe "#cache" do
      it "activities query caching on all connections" do
        master_connection.should_receive(:cache).and_yield
        slave_connection.should_receive(:cache).and_yield
        master_connection.should_not_receive(:select_value)
        slave_connection.should_receive(:select_value)

        adapter_connection.cache do
          adapter_connection.select_value("SELECT 42")
        end
      end
    end

    describe "#uncached" do
      it "deactivates query caching on all connections" do
        master_connection.should_receive(:uncached).and_yield
        slave_connection.should_receive(:uncached).and_yield
        master_connection.should_not_receive(:select_value)
        slave_connection.should_receive(:select_value)

        adapter_connection.uncached do
          adapter_connection.select_value("SELECT 42")
        end
      end
    end

    describe "#clear_query_cache" do
      it "clears the query cache on all connections" do
        master_connection.should_receive(:clear_query_cache)
        slave_connection.should_receive(:clear_query_cache)

        adapter_connection.clear_query_cache
      end
    end
  end
end
