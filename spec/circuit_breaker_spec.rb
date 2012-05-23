$: << File.expand_path(File.join(File.dirname( __FILE__ ), '..', 'lib'))

require 'rspec'
require 'active_record/connection_adapters/master_slave_adapter/circuit_breaker'

describe ActiveRecord::ConnectionAdapters::MasterSlaveAdapter::CircuitBreaker do
  let(:logger) { nil }
  let(:failure_threshold) { 5 }
  let(:timeout) { 10 }

  subject { described_class.new(logger, failure_threshold, timeout) }

  it 'should not be tripped by default' do
    should_not be_tripped
  end

  context "after single failure" do
    before { subject.fail! }

    it 'should remain untripped' do
      should_not be_tripped
    end
  end

  context "after failure threshold is reached" do
    before { failure_threshold.times { subject.fail! } }

    it { should be_tripped }

    context "and timeout exceeded" do
      before do
        now = Time.now
        Time.stub(:now).and_return(now + timeout)
        subject.tripped? # side effect :/
      end

      it { should_not be_tripped }

      context "after single failure" do
        before { subject.fail! }

        it { should be_tripped }
      end

      context "after single success" do
        before { subject.success! }

        it { should_not be_tripped }
      end
    end
  end
end
