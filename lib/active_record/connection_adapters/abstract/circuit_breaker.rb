module ActiveRecord
  module ConnectionAdapters
    class CircuitBreaker
      def initialize(logger = nil, failure_threshold = 5, timeout = 30)
        @logger = logger
        @failure_count = 0
        @failure_threshold = failure_threshold
        @timeout = timeout
        @state = :closed
      end

      def tripped?
        if open? && timeout_exceeded?
          change_state_to :half_open
        end

        open?
      end

      def success!
        if !closed?
          @failure_count = 0
          change_state_to :closed
        end
      end

      def fail!
        @failure_count += 1
        if !open? && @failure_count >= @failure_threshold
          @opened_at = Time.now
          change_state_to :open
        end
      end

    private

      def open?
        :open == @state
      end

      def half_open?
        :half_open == @state
      end

      def closed?
        :closed == @state
      end

      def timeout_exceeded?
        (Time.now - @opened_at) >= @timeout
      end

      def change_state_to(state)
        @state = state
        @logger && @logger.warn("circuit is now #{state}")
      end
    end
  end
end
