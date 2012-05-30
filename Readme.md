# Replication Aware Master Slave Adapter [![Build Status](https://secure.travis-ci.org/soundcloud/master_slave_adapter.png)][6]

Improved version of the [master_slave_adapter plugin][1], packaged as a gem.

## Features

1. automatic selection of master or slave connection: `with_consistency`
2. manual selection of master or slave connection: `with_master`, `with_slave`
3. handles master unavailable scenarios gracefully
4. transaction callbacks: `on_commit`, `on_rollback`
5. also:
  * support for multiple slaves
  * (partial) support for [database_cleaner][2]

### Automatic Selection of Master or Slave

The adapter will run all reads against a slave database, unless a) the read is inside an open transaction or b) the
adapter determines that the slave lags behind the master _relative to the last write_. For this to work, an initial
initial consistency requirement, a Clock, must be passed to the adapter. Based on this clock value, the adapter
determines if a (randomly chosen) slave meets this requirement. If not, all statements are executed against master,
otherwise, the slave connection is used until either a transaction is opened or a write occurs. After a successful write
or transaction, the adapter determines a new consistency requirement, which is returned and can be used for subsequent
operations. Note that after a write or transaction, the adapter keeps using the master connection.

As an example, a Rails application could run the following function as an `around_filter`:

```ruby
def with_consistency_filter
  if logged_in?
    clock = cached_clock_for(current_user)

    new_clock = ActiveRecord::Base.with_consistency(clock) do
      # inside the controller, ActiveRecord models can be used just as normal.
      # The adapter will take care of choosing the right connection.
      yield
    end

    [ new_clock, clock ].compact.max.tap do |c|
      cache_clock_for(current_user, c)
    end if new_clock != clock
  else
    # anonymous users will have to wait until the slaves have caught up
    with_slave { yield }
  end
end
```

Note that we use the last seen consistency for a given user as reference point. This will give the user a recent view of the data,
possibly reading from master, and if no write occurs inside the `with_consistency` block, we have a reasonable value to
cache and reuse on subsequent requests.
If no cached clock is available, this indicates that no particular consistency is required. Any slave connection will do.
Since `with_consistency` blocks can be nested, the controller code could later decide to require a more recent view on
the data.

_See also this [blog post][3] for a more detailed explanation._

### Manual Selection of Master or Slave

The original functionality of the adapter has been preserved:

```ruby
ActiveRecord::Base.with_master do
  # everything inside here will go to master
end

ActiveRecord::Base.with_slave do
  # everything inside here will go to one of the slaves
  # opening a transaction or writing will switch to master
  # for the rest of the block
end
```

`with_master`, `with_slave` as well as `with_consistency` can be nested deliberately.

### Handles master unavailable scenarios gracefully

Due to scenarios when the master is possibly down (e.g., maintenance), we try
to delegate as much as possible to the active slaves. In order to accomplish
this we have added the following functionalities.

 * We ignore errors while connecting to the master server.
 * ActiveRecord::MasterUnavailable exceptions are raised in cases when we need to use
   a master connection, but the server is unavailable. This exception is propagated
   to the application.
 * We have introduced the circuit breaker pattern in the master reconnect logic
   to prevent excessive reconnection attempts. We block any queries which require
   a master connection for a given timeout (by default, 30 seconds). After the
   timeout has expired, any attempt of using the master connection will trigger
   a reconnection.
 * The master slave adapter is still usable for any queries that require only
   slave connections.

### Transaction Callbacks

This feature was originally developed at [SoundCloud][4] for the standard `MysqlAdapter`. It allows arbitrary blocks of
code to be deferred for execution until the next transaction completes (or rolls back).

```irb
irb> ActiveRecord::Base.on_commit { puts "COMMITTED!" }
irb> ActiveRecord::Base.on_rollback { puts "ROLLED BACK!" }
irb> ActiveRecord::Base.connection.transaction do
irb*   # ...
irb> end
COMMITTED!
=> nil
irb> ActiveRecord::Base.connection.transaction do
irb*   # ...
irb*   raise "failed operation"
irb> end
ROLLED BACK!
# stack trace omitted
=> nil
```

Note that a transaction callback will be fired only *once*, so you might want to do:

```ruby
class MyModel
  after_save do
    connection.on_commit do
      # ...
    end
  end
end
```

### Support for Multiple Slaves

The adapter keeps a list of slave connections (see *Configuration*) and chooses randomly between them. The selection is
made at the beginning of a `with_slave` or `with_consistency` block and doesn't change until the block returns. Hence, a
nested `with_slave` or `with_consistency` might run against a different slave.

### Database Cleaner

At [SoundCloud][4], we're using [database_cleaner][2]'s 'truncation strategy' to wipe the database between [cucumber][5]
'feature's. As our cucumber suite proved valuable while testing the `with_consistency` feature, we had to support
`truncate_table` as an `ActiveRecord::Base.connection` instance method. We might add other strategies if there's enough
interest.

## Requirements

MasterSlaveAdapter requires ActiveRecord with a version >= 2.3, is compatible
with at least Ruby 1.8.7, 1.9.2, 1.9.3 and comes with built-in support for mysql
and mysql2 libraries.

You can check the versions it's tested against at [Travis CI](http://travis-ci.org/#!/soundcloud/master_slave_adapter).

## Installation

Using plain rubygems:

    $ gem install master_slave_adapter

Using bundler, just include it in your Gemfile:

    gem 'master_slave_adapter'

## Configuration

Example configuration for the development environment in `database.yml`:

```yaml
development:
  adapter: master_slave          # use master_slave adapter
  connection_adapter: mysql      # actual adapter to use (only mysql is supported atm)
  disable_connection_test: false # when an instance is checked out from the connection pool,
                                 # we check if the connections are still alive, reconnecting if necessary

  # these values are picked up as defaults in the 'master' and 'slaves' sections:
  database: aweapp_development
  username: aweappuser
  password: s3cr3t

  master:
    host: masterhost
    username: readwrite_user     # override default value

  slaves:
    - host: slave01
    - host: slave02
```

## Credits

* Maurício Lenhares - _original master_slave_adapter plugin_
* Torsten Curdt     - _with_consistency, maintainership & open source licenses_
* Sean Treadway     - _chief everything & transaction callbacks_
* Kim Altintop      - _strong lax monoidal endofunctors_
* Omid Aladini      - _chief operator & everything else_
* Tiago Loureiro    - _review expert & master unavailable handling_
* Tobias Schmidt    - _typo master & activerecord ranter_


[1]: https://github.com/mauricio/master_slave_adapter
[2]: https://github.com/bmabey/database_cleaner
[3]: http://www.yourdailygeekery.com/2011/06/14/master-slave-consistency.html
[4]: http://backstage.soundcloud.com
[5]: http://cukes.info
[6]: http://travis-ci.org/soundcloud/master_slave_adapter