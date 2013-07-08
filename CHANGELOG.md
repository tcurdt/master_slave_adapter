# 1.1.2 (November 26, 2012)

  * Avoid trying to connect to master twice if unavailable

# 1.1.1 (November 17, 2012)

  * [BUGFIX] Fix activerecord 3.2 compatibility
  * Fix setup of mysql integration servers

# 1.1.0 (November 15, 2012)

  * [BUGFIX] Don't raise MasterUnavailable if a slave is unavailable

# 1.0.0 (July 24, 2012)

  * Add support for unavailable master connection
  * Restrict the public interface. Removed the following methods:
    * all class methods from ActiveRecord::ConnectionAdapters::MasterSlaveAdapter
    * #current_connection=
    * #current_clock=
    * #slave_consistent?
    * ActiveRecord::Base.on_commit and ActiveRecord::Base.on_rollback
  * Fix 1.8.7 compliance
  * Fix bug which led to infinitely connection stack growth
  * Add ActiveRecord 3.x compatibility
  * Add support for Mysql2

# 0.2.0 (April 2, 2012)

  * Add support for ActiveRecord's query cache

# 0.1.10 (March 06, 2012)

  * Delegate #visitor to master connection
