$: << File.expand_path(File.join(File.dirname(__FILE__), '..', '..', 'lib'))

require 'rspec'
require 'master_slave_adapter'
require 'integration/helpers/shared_mysql_examples'

describe "ActiveRecord::ConnectionAdapters::Mysql2MasterSlaveAdapter" do
  let(:connection_adapter) { 'mysql2' }

  it_should_behave_like "a MySQL MasterSlaveAdapter"
end
