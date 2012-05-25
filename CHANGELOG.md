# 1.0.0 (not released yet)

* Add support for unavailable master connection
* Fallback to slave connection if possible
* Restrict the public interface. Removed the following methods:
  * all class methods from ActiveRecord::ConnectionAdapters::MasterSlaveAdapter
  * #current_connection=
  * #current_clock=
  * #slave_consistent?
  * ActiveRecord::Base.on_commit and ActiveRecord::Base.on_rollback
* Fix 1.8.7 compliance

# 0.2.0 (April 2, 2012)

* Add support for ActiveRecord's query cache

# 0.1.10 (March 06, 2012)

* Delegate #visitor to master connection
