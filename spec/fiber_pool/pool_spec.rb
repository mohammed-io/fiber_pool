# frozen_string_literal: true

require 'redis'
require 'async'
require 'async/barrier'
require 'async/redis'

slow_lua_script = %{
    local wait_time = tonumber(ARGV[1]) or 1000
    redis.call('PING')
    local start = redis.call('TIME')
    local end_time = (start[1] * 1000000 + start[2]) + wait_time * 1000

    while true do
        local now = redis.call('TIME')
        local current_time = now[1] * 1000000 + now[2]
        if current_time >= end_time then
            break
        end
    end

    return "Waited for " .. wait_time .. " milliseconds."
}

RSpec.describe FiberPool::Pool do
  describe '#checkout' do
    it 'returns the connection object' do
      pool = FiberPool::Pool.new(1) { :connection }
      expect(pool.checkout).to eq(:connection)
    end
  end

  describe '#checkin' do
    it 'returns a connection to the pool' do
      pool = FiberPool::Pool.new(1) { :connection }
      connection = pool.checkout
      pool.checkin(connection)
      expect(pool.checkout).to eq(:connection)
    end
  end

  describe '#with_connection' do
    it 'yields a connection' do
      pool = FiberPool::Pool.new(1) { :connection }
      pool.with_connection do |connection|
        expect(connection).to eq(:connection)
      end
    end

    it 'raise EmptyConnectionPoolError when the pool is empty' do
      pool = FiberPool::Pool.new(1) { :connection }
      pool.checkout
      expect { pool.checkout }.to raise_error(FiberPool::Pool::EmptyConnectionPoolError)
    end

    it 'queues the block when the pool is empty' do
      pool = FiberPool::Pool.new(1) { :connection }
      pool.checkout
      pool.with_connection do
        expect(pool.queue.size).to eq(1)
      end
    end

    context 'with async redis' do
      it 'allocates max of 5 connections and re-use those connections' do
        pool = FiberPool::Pool.new(5) do
          Async::Redis::Client.new('redis://localhost:11223/0')
        end

        all_connections = []

        t_i = DateTime.now.to_time
        Async do
          10.times.map do
            Thread.new do
              pool.with_connection do |redis|
                all_connections << redis.object_id

                # FOR REVIEW: The async redis seems to not wait for this operation to finish
                # How can I await it while keep it non blocking?
                redis.eval(slow_lua_script, [], [300]) # Not useful yet
                redis.set('foo', 'bar')
                expect(redis.get('foo')).to eq('bar')
              rescue RedisClient::CommandError, Redis::CommandError
                puts 'RedisClient::CommandError'
              rescue StandardError => e
                puts e
              end
            end
          end.map(&:join)
        end

        expect(all_connections.size).to eq(10)
        expect(all_connections.uniq.size).to eq(5)
      ensure
        puts DateTime.now.to_time - t_i
      end
    end

    context 'with redis' do
      before do
        # A separate redis server to not interfere with the default redis server on your machine
        # $redis-server --port 11223 --maxclients 5

        expect { Redis.new(url: 'redis://localhost:11223/0').ping }.not_to raise_error do
          puts 'Make sure to run redis server with the correct port and max clients: redis-server --port 11223 --maxclients 5'
        end
      end

      it 'allocates max of 5 connections and re-use those connections' do
        pool = FiberPool::Pool.new(5) do
          Redis.new(url: 'redis://localhost:11223/0', read_timeout: 5.0)
        end

        all_connections = []

        t_i = DateTime.now.to_time
        Async do
          10.times.map do
            Thread.new do
              pool.with_connection do |redis|
                all_connections << redis.object_id

                redis.eval(slow_lua_script, [], [300]) # Not useful yet
                redis.set('foo', 'bar')
                expect(redis.get('foo')).to eq('bar')
              rescue RedisClient::CommandError, Redis::CommandError
                puts 'RedisClient::CommandError'
              rescue StandardError => e
                puts e
              end
            end
          end.map(&:join)
        end

        expect(all_connections.size).to eq(10)
        expect(all_connections.uniq.size).to eq(5)
      ensure
        puts DateTime.now.to_time - t_i
      end
    end
  end
end
