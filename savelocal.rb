require_relative 'lib/tsstream'

def save(name)
    puts name
    basename = File.dirname name
    ts = TSStream.new name
    threads = []
    while ts.nextchunk do
        chunkname = ts.chunk_name
        chunkdata = ts.chunk_payload
        threads << Thread.new do 
            puts "writing #{chunkname}"
            File.write "#{basename}/#{chunkname}", chunkdata
        end
    end
    threads.each { |t| t.join  }

    File.write "#{name}.m3u8", ts.playlist

end
