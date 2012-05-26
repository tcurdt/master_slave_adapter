$: << File.expand_path(File.join(File.dirname(__FILE__), '..', '..', 'lib'))

require 'rspec'
require 'mysql'
require 'timeout'
require 'active_record/connection_adapters/mysql_master_slave_adapter'

require './spec/integration/helpers/mysql_helper'

describe ActiveRecord::ConnectionAdapters::MysqlMasterSlaveAdapter do
  include MysqlHelper

  let(:configuration) do
    {
      :adapter => 'master_slave',
      :connection_adapter => 'mysql',
      :username => 'root',
      :database => 'master_slave_adapter',
      :master => {
        :host => '127.0.0.1',
        :port => port(:master),
      },
      :slaves => [{
        :host => '127.0.0.1',
        :port => port(:slave),
      }],
    }
  end

  def connection
    ActiveRecord::Base.connection
  end

  def should_read_from(host)
    server = server_id(host)
    query  = "SELECT @@Server_id as Value"

    connection.select_all(query).first["Value"].should == server
    connection.select_one(query)["Value"].should == server
    connection.select_rows(query).first.first.should == server
    connection.select_value(query).should == server
    connection.select_values(query).first.should == server
  end

  before(:all) do
    setup
    start_master
    start_slave
    configure
    start_replication
  end

  after(:all) do
    stop_master
    stop_slave
  end

  before(:each) do
    ActiveRecord::Base.establish_connection(configuration)
  end

  it "connects to the database" do
    expect { ActiveRecord::Base.connection }.to_not raise_error
  end

  context "when asked for master" do
    it "reads from master" do
      ActiveRecord::Base.with_master do
        should_read_from :master
      end
    end
  end

  context "when asked for slave" do
    it "reads from slave" do
      ActiveRecord::Base.with_slave do
        should_read_from :slave
      end
    end
  end

  context "when asked for consistency" do
    context "given slave is fully synced" do
      before do
        wait_for_replication_sync
      end

      it "reads from slave" do
        ActiveRecord::Base.with_consistency(connection.master_clock) do
          should_read_from :slave
        end
      end
    end

    context "given slave lags behind" do
      before do
        stop_replication
        move_master_clock
      end

      after do
        start_replication
      end

      it "reads from master" do
        ActiveRecord::Base.with_consistency(connection.master_clock) do
          should_read_from :master
        end
      end

      context "and slave catches up" do
        before do
          start_replication
          wait_for_replication_sync
        end

        it "reads from slave" do
          ActiveRecord::Base.with_consistency(connection.master_clock) do
            should_read_from :slave
          end
        end
      end
    end
  end

  context "given master is not available" do
    before(:all) do
      stop_master
    end

    after(:all) do
      start_master
    end

    context "when asked for master" do
      it "fails" do
        expect do
          ActiveRecord::Base.with_master { read }
        end.to raise_error(ActiveRecord::MasterUnavailable)
      end
    end

    context "when asked for slave" do
      it "reads from slave" do
        ActiveRecord::Base.with_slave do
          should_read_from :slave
        end
      end
    end
  end
end
