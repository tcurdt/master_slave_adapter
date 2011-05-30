# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "master_slave_adapter/version"

Gem::Specification.new do |s|
  s.name        = 'master_slave_adapter_tcurdt'
  s.version     = Mygem::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = [ 'Mauricio Linhares', 'Torsten Curdt' ]
  s.email       = 'tcurdt at vafer.org'
  s.homepage    = 'http://github.com/tcurdt/master_slave_adapter_mauricio'
  s.summary     = 'Master Slave Adapter'
  s.description = 'Acts as a ActiveRecord adapter and allows you to setup a master-slave environment.'

  s.rubyforge_project = "master_slave_adapter_tcurdt"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = [ "lib" ]

  s.add_dependency('activerecord', [ "= 2.3.9" ])
end