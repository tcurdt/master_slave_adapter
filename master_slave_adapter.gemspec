# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)

Gem::Specification.new do |s|
  s.name        = 'master_slave_adapter_soundcloud'
  s.version     = '0.1.8' 
  s.date        = '2011-11-15'
  s.platform    = Gem::Platform::RUBY
  s.authors     = [ 'Mauricio Linhares', 'Torsten Curdt', 'Kim Altintop', 'Omid Aladini', 'SoundCloud' ]
  s.email       = %q{kim@soundcloud.com tcurdt@soundcloud.com omid@soundcloud.com}
  s.homepage    = 'http://github.com/soundcloud/master_slave_adapter'
  s.summary     = %q{Replication Aware Master/Slave Database Adapter for Rails/ActiveRecord}
  s.description = %q{(MySQL) Replication Aware Master/Slave Database Adapter for Rails/ActiveRecord}

  s.files        = `git ls-files`.split("\n")
  s.test_files   = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables  = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_path = 'lib'

  s.required_ruby_version     = '>= 1.9.2'
  s.required_rubygems_version = '>= 1.3.7'
  s.add_development_dependency 'rspec'

  s.add_dependency 'activerecord', '>= 2.3.9'
  # = MANIFEST =
  s.files = %w[
    Gemfile
    LICENSE
    Rakefile
    Readme.md
    lib/active_record/connection_adapters/master_slave_adapter.rb
    lib/master_slave_adapter.rb
    master_slave_adapter.gemspec
    specs/specs.rb
  ]
  # = MANIFEST =
end
