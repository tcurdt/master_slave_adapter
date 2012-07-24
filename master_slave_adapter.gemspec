$:.push File.expand_path("../lib", __FILE__)

require 'active_record/connection_adapters/master_slave_adapter/version'

Gem::Specification.new do |s|
  s.name        = 'master_slave_adapter'
  s.version     = ActiveRecord::ConnectionAdapters::MasterSlaveAdapter::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = [ 'Mauricio Linhares', 'Torsten Curdt', 'Kim Altintop', 'Omid Aladini', 'Tiago Loureiro', 'Tobias Schmidt', 'SoundCloud' ]
  s.email       = %q{tiago@soundcloud.com ts@soundcloud.com}
  s.homepage    = 'http://github.com/soundcloud/master_slave_adapter'
  s.summary     = %q{Replication Aware Master/Slave Database Adapter for ActiveRecord}
  s.description = %q{(MySQL) Replication Aware Master/Slave Database Adapter for ActiveRecord}

  s.files        = `git ls-files`.split("\n")
  s.test_files   = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables  = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_path = 'lib'

  s.required_ruby_version     = '>= 1.8.7'
  s.required_rubygems_version = '>= 1.3.7'

  s.add_dependency 'activerecord', ['>= 2.3.9', '< 4.0']

  s.add_development_dependency 'rake'
  s.add_development_dependency 'rspec'
end
