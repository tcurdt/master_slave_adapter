module ActiveRecord
  module ConnectionAdapters
    module MasterSlaveAdapter
      class Clock
        include Comparable
        attr_reader :file, :position

        def initialize(file, position)
          raise ArgumentError, "file and postion may not be nil" if file.nil? || position.nil?
          @file, @position = file, position.to_i
        end

        def <=>(other)
          @file == other.file ? @position <=> other.position : @file <=> other.file
        end

        def to_s
          [ @file, @position ].join('@')
        end

        def infinity?
          self == self.class.infinity
        end

        def self.zero
          @zero ||= Clock.new('', 0)
        end

        def self.infinity
          @infinity ||= Clock.new('', Float::MAX.to_i)
        end

        def self.parse(string)
          new(*string.split('@'))
        rescue
          nil
        end
      end
    end
  end
end
