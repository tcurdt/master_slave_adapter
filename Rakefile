require 'bundler'
require 'rspec/core/rake_task'

Bundler::GemHelper.install_tasks

desc 'Run specs'
task :spec => ['spec:common', 'spec:integration']

namespace :spec do
  desc 'Run common specs'
  RSpec::Core::RakeTask.new(:common) do |task|
    task.pattern = './spec/*_spec.rb'
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

    desc 'Run all'
    RSpec::Core::RakeTask.new(:all) do |task|
      task.pattern = './spec/integration/*_spec.rb'
      task.verbose = false
    end
  end
end

desc 'Default: Run specs'
task :default => :spec
