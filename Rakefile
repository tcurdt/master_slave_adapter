require 'bundler'
require 'rspec/core/rake_task'

Bundler::GemHelper.install_tasks

class MasterSlaveAdapterRSpecTask < RSpec::Core::RakeTask
  attr_accessor :exclude

private

  def files_to_run
    FileList[ pattern ].exclude(exclude)
  end
end

def mysql2_adapter_available?
  require 'active_record/connection_adapters/mysql2_adapter'
  true
rescue LoadError
  false
rescue
  true
end

desc 'Default: Run specs'
task :default => :spec

desc 'Run specs'
task :spec => ['spec:common', 'spec:integration']

namespace :spec do
  desc 'Run common specs'
  MasterSlaveAdapterRSpecTask.new(:common) do |task|
    task.pattern = './spec/*_spec.rb'
    task.exclude = /mysql2/ unless mysql2_adapter_available?
    task.verbose = false
  end

  desc 'Run integration specs'
  task :integration => ['spec:integration:check', 'spec:integration:all']

  namespace :integration do
    desc 'Check requirements'
    task :check do
      [:mysql, :mysqld, :mysql_install_db].each do |executable|
        unless system("which #{executable} > /dev/null")
          raise "Can't run integration tests. #{executable} is not available in $PATH"
        end
      end
    end

    desc 'Run all integration specs'
    MasterSlaveAdapterRSpecTask.new(:all) do |task|
      task.pattern = './spec/integration/*_spec.rb'
      task.exclude = /mysql2/ unless mysql2_adapter_available?
      task.verbose = false
    end
  end
end
