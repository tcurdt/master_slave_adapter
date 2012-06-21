require 'active_record/connection_adapters/master_slave_adapter'

SchemaStatements = ActiveRecord::ConnectionAdapters::SchemaStatements.public_instance_methods.map(&:to_sym)
SelectMethods = [ :select_all, :select_one, :select_rows, :select_value, :select_values ]

shared_context 'connection setup' do
  let(:default_database_setup) do
    {
      :adapter => 'master_slave',
      :username => 'root',
      :database => 'slave',
      :connection_adapter => connection_adapter,
      :master => { :username => 'root', :database => 'master' },
      :slaves => [{ :database => 'slave' }],
    }
  end

  let(:database_setup) do
    default_database_setup
  end

  let(:mocked_methods) do
    {
      :reconnect!  => true,
      :disconnect! => true,
      :active?     => true,
    }
  end

  let(:master_connection) do
    stubs = mocked_methods.merge(:open_transactions => 0)
    mock('master connection', stubs).tap do |conn|
      conn.stub(:uncached).and_yield
    end
  end

  let(:slave_connection) do
    mock('slave connection', mocked_methods).tap do |conn|
      conn.stub(:uncached).and_yield
    end
  end

  before do
    ActiveRecord::Base.master_mock = master_connection
    ActiveRecord::Base.slave_mock = slave_connection
    ActiveRecord::Base.establish_connection(database_setup)
  end

  after do
    ActiveRecord::Base.connection_handler.clear_all_connections!
  end

  def adapter_connection
    ActiveRecord::Base.connection
  end
end
