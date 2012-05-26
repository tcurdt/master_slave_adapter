require 'fileutils'

module MysqlHelper
  MASTER_ID   = "1"
  MASTER_PORT = 3310
  SLAVE_ID    = "2"
  SLAVE_PORT  = 3311
  TEST_TABLE  = "master_slave_adapter.master_slave_test"

  def port(identifier)
    case identifier
    when :master then MASTER_PORT
    when :slave  then SLAVE_PORT
    end
  end

  def server_id(identifier)
    case identifier
    when :master then MASTER_ID
    when :slave  then SLAVE_ID
    end
  end

  def start_replication
    execute(:slave, "start slave")
  end

  def stop_replication
    execute(:slave, "stop slave")
  end

  def move_master_clock
    execute(:master, "insert into #{TEST_TABLE} (message) VALUES ('test')")
  end

  def wait_for_replication_sync
    Timeout.timeout(5) do
      until slave_status == master_status; end
    end
  rescue Timeout::Error
    raise "Replication synchronization failed"
  end

  def configure
    execute(:master, <<-EOS)
      SET sql_log_bin = 0;
      create user 'slave'@'localhost' identified by 'slave';
      grant replication slave on *.* to 'slave'@'localhost';
      create database master_slave_adapter;
      SET sql_log_bin = 1;
    EOS

    execute(:slave, <<-EOS)
      change master to master_user = 'slave',
             master_password = 'slave',
             master_port = #{port(:master)},
             master_host = 'localhost';
      create database master_slave_adapter;
    EOS

    execute(:master, <<-EOS)
      CREATE TABLE #{TEST_TABLE} (
        id int(11) NOT NULL AUTO_INCREMENT,
        message text COLLATE utf8_unicode_ci,
        created_at datetime DEFAULT NULL,
        PRIMARY KEY (id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
    EOS
  end

  def setup
    [:master, :slave].each do |name|
      path = location(name)
      config_path = File.join(path, "my.cnf")
      data_path = File.join(path, "data")

      FileUtils.rm_rf(path)
      FileUtils.mkdir_p(path)
      File.open(config_path, "w") { |file| file << config(name) }

      `mysql_install_db --basedir=/usr/local --datadir="#{data_path}" --user=\$USER`
    end
  end

  def start_master
    start(:master)
  end

  def stop_master
    stop(:master)
  end

  def start_slave
    start(:slave)
  end

  def stop_slave
    stop(:slave)
  end

private

  def slave_status
    status(:slave)[5..6]
  end

  def master_status
    status(:master)[0..1]
  end

  def status(name)
    `mysql --protocol=TCP -P#{port(name)} -uroot -N -s -e 'show #{name} status'`.strip.split("\t")
  end

  def execute(host, statement = '')
    system(%{mysql --protocol=TCP -P#{port(host)} -uroot -e "#{statement}"})
  end

  def start(name)
    $pipes ||= {}
    $pipes[name] = IO.popen("mysqld --defaults-file='#{location(name)}/my.cnf'")
    wait_for_database_boot(name)
  end

  def stop(name)
    pipe = $pipes[name]
    Process.kill("TERM", pipe.pid)
  ensure
    pipe.close unless pipe.closed?
  end

  def started?(host)
    system(%{mysql --protocol=TCP -P#{port(host)} -uroot -e '' 2> /dev/null})
  end

  def wait_for_database_boot(host)
    Timeout.timeout(5) do
      until started?(host); end
    end
  rescue Timeout::Error
    raise "Couldn't connect to MySQL"
  end

  def location(name)
    File.expand_path(File.join("..", "database", name.to_s), File.dirname(__FILE__))
  end

  def config(name)
    path = location(name)

    <<-EOS
[mysqld]
pid-file = #{path}/mysqld.pid
socket = #{path}/mysqld.sock
port = #{port(name)}
log_output = FILE
log-error = #{path}/error.log
datadir = #{path}/data
log-bin = #{name}-bin
log-bin-index = #{name}-bin.index
server-id = #{server_id(name)}
lower_case_table_names = 2
    EOS
  end
end
