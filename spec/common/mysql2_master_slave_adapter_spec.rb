$: << File.expand_path(File.join(File.dirname( __FILE__ ), '..', '..', 'lib'))

require 'rspec'
require 'common/support/connection_setup_helper'
require 'common/support/mysql_consistency_examples'
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
  include_context 'connection setup'
  let(:connection_adapter) { 'mysql2' }

  it_should_behave_like 'mysql consistency'

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
        ActiveRecord::Base.should_receive(:master_mock).and_raise(error)

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
