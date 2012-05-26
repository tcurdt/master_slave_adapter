require 'bundler'
require 'rspec/core/rake_task'

Bundler::GemHelper.install_tasks

desc 'Run all specs'
task :spec => ['spec:common', 'spec:integration']

namespace :spec do
  desc 'Run common tests'
  RSpec::Core::RakeTask.new(:common) do |task|
    task.pattern = './spec/*_spec.rb'
    task.verbose = false
  end

  desc 'Run integration tests'
  RSpec::Core::RakeTask.new(:integration) do |task|
    task.pattern = './spec/integration/*_spec.rb'
    task.verbose = false
  end
end

desc 'Default: Run common specs'
task :default => 'spec:common'
