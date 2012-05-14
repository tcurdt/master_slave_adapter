# Replication Aware Master Slave Adapter [![Build Status](https://secure.travis-ci.org/soundcloud/large-hadron-migrator.png)][6]

Improved version of the [master_slave_adapter plugin][1], packaged as a gem.

## Features

1. automatic selection of master or slave connection: `with_consistency`
2. manual selection of master or slave connection: `with_master`, `with_slave`
3. transaction callbacks: `on_commit`, `on_rollback`
4. also:
  * support for multiple slaves
  * (partial) support for [database_cleaner][2]

### Automatic Selection of Master or Slave

* _note that this feature currently only works with MySQL_
* _see also this [blog post][3] for a more detailed explanation_

The adapter will run all reads against a slave database, unless a) the read is inside an open transaction or b) the
adapter determines that the slave lags behind the master _relative to the last write_. For this to work, an initial
initial consistency requirement ("`Clock`") must be passed to the adapter. Based on this clock value, the adapter
determines if a (randomly chosen) slave meets this requirement. If not, all statements are executed against master,
otherwise, the slave connection is used until either a transaction is opened or a write occurs. After a successful write
or transaction, the adapter determines a new consistency requirement, which is returned and can be used for subsequent
operations. Note that after a write or transaction, the adapter keeps using the master connection.

As an example, a Rails application could run the following function as an `around_filter`:

```ruby
def with_consistency_filter
  if logged_in?
    # it's a good idea to use this feature on a per-user basis
    cache_key = [ CACHE_NAMESPACE, current_user.id.to_s ].join(":")

    clock = cached_clock(cache_key) ||
      ActiveRecord::Base.connection.master_clock

    new_clock = ActiveRecord::Base.with_consistency(clock) do
        # inside the controller, ActiveRecord models can be used just as normal.
        # The adapter will take care of choosing the right connection.
        yield
      end

    [ new_clock, clock ].compact.max.tap do |c|
      cache_clock!(cache_key, c)
    end if new_clock != clock
  else
    # anonymous users will have to wait until the slaves have caught up
    with_slave { yield }
  end
end
```

Note that we use the current `master_clock` as a reference point. This will give the user a recent view of the data,
possibly reading from master, and if no write occurs inside the `with_consistency` block, we have a reasonable value to
cache and reuse on subsequent requests. Alternatively, we could have used
`ActiveRecord::ConnectionAdapters::MasterSlaveAdapter::Clock.zero` to indicate no particular consistency requirement.
Since `with_consistency` blocks can be nested, the controller code could later decide to require a more recent view on
the data.

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

## Installation

Using plain rubygems:

```sh
$ gem install master_slave_adapter_soundcloud
```

Using bundler:

```sh
$ cat >> Gemfile
gem 'master_slave_adapter_soundcloud', '~> 0.1', :require => 'master_slave_adaper'
^D
$ bundle install
```

## Credits

* Maurício Lenhares - _original master_slave_adapter plugin_
* Torsten Curdt     - _with_consistency, maintainership & open source licenses_
* Sean Treadway     - _chief everything & transaction callbacks_
* Kim Altintop      - _strong lax monoidal endofunctors_
* Omid Aladini      - _chief operator & everything else_


[1]: https://github.com/mauricio/master_slave_adapter
[2]: https://github.com/bmabey/database_cleaner
[3]: http://www.yourdailygeekery.com/2011/06/14/master-slave-consistency.html
[4]: http://backstage.soundcloud.com
[5]: http://cukes.info
[6]: http://travis-ci.org/soundcloud/master_slave_adapter