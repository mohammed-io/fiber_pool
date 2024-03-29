# frozen_string_literal: true

require 'redis'
require 'async'
require 'async/barrier'
require 'async/redis'
require 'benchmark'

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

    context 'with async redis' do
      it 'allocates max of 5 connections and re-use those connections' do
        pool = FiberPool::Pool.new(5) do
          Async::Redis::Client.new(Async::Redis.local_endpoint(port: 11_223))
        end

        all_connections = []

        Benchmark.bm do |x|
          x.report do
            Async do
              10.times.map do
                Async do
                  pool.with_connection do |redis|
                    all_connections << redis.object_id

                    # FOR REVIEW: The async redis seems to not wait for this operation to finish
                    # How can I await it while keep it non blocking?
                    # redis.eval(slow_lua_script, [], [0]) # Not useful yet
                    redis.set('foo', 'bar')
                    expect(redis.get('foo')).to eq('bar')
                  rescue Protocol::Redis::ServerError
                    puts 'Protocol::Redis::ServerError'
                    raise
                  rescue StandardError => e
                    puts e
                  end
                end
              end.map(&:wait)
            end
          end
        end

        expect(all_connections.size).to eq(10)
        expect(all_connections.uniq.size).to eq(5)
      end
    end

    context 'with redis' do
      before do
        # A separate redis server to not interfere with the default redis server on your machine
        # $redis-server --port 11223 --maxclients 5

        check_for_redis = lambda do
          client = Redis.new(url: 'redis://localhost:11223/0')
          client.ping
          client.close # The connection should be closed, because the server only allows 5 connections in our config.
        end

        expect(&check_for_redis).not_to raise_error do
          puts 'Make sure to run redis server with the correct port and max clients:
                redis-server --port 11223 --maxclients 5'
        end
      end
    end
  end
end
