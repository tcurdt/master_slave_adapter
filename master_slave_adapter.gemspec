Gem::Specification.new do |s|
  s.specification_version = 2 if s.respond_to? :specification_version=
  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.rubygems_version = '1.3.1'

  s.rubyforge_project = 'master_slave_adapter_tcurdt'
  s.name              = 'master_slave_adapter_tcurdt'
  s.version           = '0.1'
  s.date              = '2011-05-30'

  s.summary           = "Master Slave Adapter"
  s.description       = "Acts as a ActiveRecord adapter and allows you to setup a master-slave environment."

  s.authors           = [ "Mauricio Linhares", "Torsten Curdt" ]
  s.homepage          = 'http://github.com/tcurdt/master_slave_adapter_mauricio'
  s.email             = 'tcurdt@vafer.org'

  s.add_dependency('activerecord', [ "= 2.3.0" ])

  s.require_paths = %w[lib]
  s.files = %w[
    Gemfile
    init.rb
    lib/active_record/connection_adapters/master_slave_adapter.rb
    lib/master_slave_adapter/active_record_extensions.rb
    lib/master_slave_adapter/adapter.rb
    lib/master_slave_adapter/instance_methods_generation.rb
    specs/specs.rb
    README
    LICENSE
  ]

  s.rdoc_options = [ "--charset=UTF-8" ]
  s.extra_rdoc_files = %w[README]
  s.test_files = s.files.select { |path| path =~ /^spec\/*spec\.rb/ }
end