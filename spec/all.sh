#!/usr/bin/env bash

source ~/.rvm/scripts/rvm

for ruby in 1.8.7 1.9.2 1.9.3; do
  rvm use $ruby
  for gemfile in spec/gemfiles/*; do
    if [[ "$gemfile" =~ \.lock ]]; then
      continue
    fi

    BUNDLE_GEMFILE=$gemfile bundle install --quiet
    BUNDLE_GEMFILE=$gemfile bundle exec rake spec
  done
done
