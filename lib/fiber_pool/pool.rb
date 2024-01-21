# frozen_string_literal: true

module FiberPool
  # A connection pool that uses fibers
  # It queues the blocks when there are no connections available.
  # Inspired by the event loop of Node.js.
  class Pool
    class EmptyConnectionPoolError < StandardError; end

    attr_reader :queue

    def initialize(size, &session_builder)
      @size = size
      @session_builder = session_builder
      @sessions = [session_builder.call]
      @created_sessions = 1

      @semaphore = Async::Semaphore.new(size)
      @mutex = Mutex.new
    end

    def with_connection(&block)
      @semaphore.acquire do
        process(block)
      end
    end

    def checkout
      @mutex.synchronize do
        if @size > @created_sessions
          @created_sessions += 1
          @sessions << @session_builder.call
        end

        raise EmptyConnectionPoolError if @sessions.empty?

        @sessions.pop
      end
    end

    def checkin(session)
      @mutex.synchronize do
        @sessions << session
      end
    end

    private

    def process(block)
      fn = proc do
        session = checkout
        block.call(session)
      ensure
        checkin(session)
      end

      fn.call
    end
  end
end
