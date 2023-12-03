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
      @sessions = size.times.map { session_builder.call }

      @mutex = Mutex.new
      @queue = Queue.new
    end

    def with_connection(&block)
      process_or_enqueue(block)
    end

    def checkout
      @mutex.synchronize do
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

    def process_or_enqueue(block)
      if @sessions.empty?
        @queue << block
      else
        process(block)
      end
    end

    def process(block)
      fn = proc do
        session = checkout
        block.call(session)
      ensure
        checkin(session)
      end

      if Fiber.scheduler.nil?
        Fiber.new(&fn).resume
        process_next
      else
        Fiber.schedule do
          fn.call
          process_next
        end
      end
    end

    def process_next
      return if @queue.empty?

      block = @queue.pop
      process(block)
    end
  end
end
