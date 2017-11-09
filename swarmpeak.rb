# swarmpost
# Reads an mp4 file and stores it as an mpegts stream in a set of chunks in a swarm
# object store. Each chunk contains a set of HLS fragments.
# if an m3u8 file is not availble, it is generated on the fly.
# 
require 'yaml'
require 'net/http'
#require 'bundler/setup'

require 'json'

require 'redis'

require_relative 'lib/swarmbucket'
require_relative 'lib/audiowaveform'

module SwarmPeak

    def save_peak_data(name, waveformdata)
        PEAK_BUCKET.post("#{name}", waveformdata, 'application/json')
    end

    def swarmpeak(name)
        # strip 'extension'
        puts name
        target = name.sub(/\.[.\w]+$/,'.m4a')
        if REDIS.exists name
            puts 'in cache, ignoring'
            return
        end
        REDIS.set name, 'working', ex: 30

        if PEAK_BUCKET.present? name
            puts 'json exists!'
            return
        end

        snd = Sound.new(source: "#{SOURCEPATH}/#{target}")
        puts "posting #{name}"
        save_peak_data name, snd.waveform.to_json
    end
end
