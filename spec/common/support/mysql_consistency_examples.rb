require 'active_record/connection_adapters/master_slave_adapter/clock'

Clock = ActiveRecord::ConnectionAdapters::MasterSlaveAdapter::Clock

shared_examples_for 'mysql consistency' do
  def zero
    Clock.zero
  end

  def master_position(pos)
    Clock.new('', pos)
  end

  def should_report_clock(pos, connection, log_file, log_pos, sql)
    pos = Array(pos)
    values = pos.map { |p| { log_file => '', log_pos => p } }

    connection.
      should_receive(:select_one).exactly(pos.length).times.
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
       decrement_open_transactions).each do |txstmt|
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
end
