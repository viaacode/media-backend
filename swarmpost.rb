# swarmpost
# Reads an mp4 file and stores it as an mpegts stream in a set of chunks in a swarm
# object store. Each chunk contains a set of HLS fragments.
# if an m3u8 file is not availble, it is generated on the fly.
# 
require 'yaml'
require 'net/http'
require 'bundler/setup'

require 'redis'

require_relative 'lib/swarmbucket'
require_relative 'lib/tsstream'

module SwarmPost

    config = YAML.load_file 'config.yaml'
    SWARMHTTP = SwarmBucket.new config['swarmurl'], config['swarmbucket']
    SOURCEPATH = "http://#{config['swarmurl']}/#{config['sourcebucket']}"

    # Retention times
    RET_M3U8 = 604800 # 7 days
    RET_TS_0 = RET_M3U8 + 14400
    RET_TS = 86400 # 1 day

    REDIS = Redis.new  host: config['redishost']

    def save_fragment(name, index, buffer)
        retention = index == 0 ? RET_TS_0 : RET_TS
        SWARMHTTP.post("#{name}.ts.#{index}", buffer, 'video/mpegts',retention)
    end

    def present?(name)
        return true if REDIS.exists name
        ttl = SWARMHTTP.present? name
        return false unless ttl
        expiry = ttl.is_a?(Fixnum) ? ttl : 86400
        REDIS.set(name, expiry.to_s, ex: expiry) if expiry > 10
        true
    end

    def swarmpost(name)
        puts name

        # strip 'extension'
        target = name.sub(/\.[.\w]+$/,'')
        m3u8_url = "#{target}.m3u8"
        source_url = "#{target}.mp4"

        if REDIS.exists target
            puts 'in cache, ignoring'
            return
        end
        puts :working
        REDIS.set target, 'working', ex: 60

        m3u8 = SWARMHTTP.get m3u8_url
        m3u8_file = m3u8.body if Net::HTTPSuccess === m3u8
        puts m3u8_file ? 'm3u8_file found' : 'm3u8_file not found'
        if m3u8_file && present?("#{target}.ts.1")
            puts 'ts exists, nothing to do'
            return
        end

        ts = TSStream.new "#{SOURCEPATH}/#{source_url}", m3u8_file
        threads = []
        while ts.nextchunk do
            chunkid = ts.chunk_id
            chunkpayload = ts.chunk_payload
            puts "posting #{ts.chunk_name}"
            threads << Thread.new do 
                save_fragment target, chunkid, chunkpayload
            end
        end
        threads.each { |t| t.join  }

        puts 'posting m3u8_file'
        SWARMHTTP.post m3u8_url,ts.playlist, 'application/x-mpegurl', RET_M3U8 unless m3u8_file
        REDIS.set target, 'finished', ex: RET_TS/2

    end
end