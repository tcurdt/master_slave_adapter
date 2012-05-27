#!/usr/bin/env bash

source ~/.rvm/scripts/rvm

for ruby in 1.8.7 1.9.2 1.9.3; do
  rvm use $ruby
  for gemfile in spec/gemfiles/activerecord*-mysql; do
    BUNDLE_GEMFILE=$gemfile bundle install
    BUNDLE_GEMFILE=$gemfile bundle exec rake spec
  done
done
