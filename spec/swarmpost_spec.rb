require_relative 'helpers/m3u8'
require_relative '../swarmpost'
include SwarmPost
include  TS_segemter_helper

describe "SwarmPost::post" do

    let (:ts_stream) do
        File.open 'spec/fixtures/BigBuckBunny_320x180.ts', 'rb'
    end
    let (:swarmhttp) { instance_double(SwarmBucket) }
    let (:redis) { instance_double(Redis, exists: false, set: true, del: true) }

    before :all do
        @exp_playlist = m3u8_playlist
        @exp_chunk_sizes = m3u8_chunk_sizes(@exp_playlist)
    end

    before :each do
        stub_ts_parameters
        stub_const 'SwarmPost::SWARMHTTP', swarmhttp
        stub_const 'SwarmPost::REDIS', redis
        allow(Thread).to receive(:new).and_yield.and_return( double 'Thread',join: true )
        allow(Process).to receive(:wait)
        expect(swarmhttp).to receive(:get).with('path/browse.m3u8') { m3u8 }
        allow(swarmhttp).to receive(:present?) { false }
        allow(swarmhttp).to receive(:post).with(/m3u8$/, any_args)
        allow(swarmhttp).to receive(:post).with(/ts\.\d+$/, any_args) do |name, body, header|
            @tsobject = '' unless @tsobject
            @postsize = [] unless @postsize
            @tsobject += body
            fragment = /ts\.(\d+)$/.match(name)[1].to_i
            @postsize[fragment] = body.length
            fragment == 1 ? post_response_1 : Net::HTTPSuccess.new(1.0, '200', "OK")
        end
        allow(IO).to receive(:popen).with(/.*ffmpeg -y -i http:\/\/mediahaven.prd.dg.viaa.be\/viaa\/path\/browse.mp4 -c copy -f mpegts pipe:1 <\/dev\/null 2>>log\/ffmpeg.log/) { ts_stream }
    end

    after (:each) do
      ts_stream.close
    end

    subject { swarmhttp }

    shared_examples 'segment post agent' do
        it 'posts the first fragment with a long retention' do
            is_expected.to have_received(:post)
            .with('path/browse.ts.0',any_args, 'video/mpegts',SwarmPost::RET_TS_0)
        end
        it 'posts the remaining fragments' do
            is_expected.to have_received(:post)
            .with('path/browse.ts.1',any_args, 'video/mpegts',SwarmPost::RET_TS)
            is_expected.to have_received(:post)
            .with('path/browse.ts.2',any_args, 'video/mpegts',SwarmPost::RET_TS)
            is_expected.to have_received(:post)
            .with('path/browse.ts.3',any_args, 'video/mpegts',SwarmPost::RET_TS)
        end
        it "concatenation of posted fragments matches the original stream" do
            expect(@tsobject).to eq IO.binread('spec/fixtures/BigBuckBunny_320x180.ts')
        end
        it "sizes of posted fragments match sum of byteranges" do
            expect(@postsize).to eq @exp_chunk_sizes
        end
    end

    context 'when the m3u8 playlist does not exist' do
        let (:m3u8) { Net::HTTPNotFound.new 1.0, '404', 'Not Found' }
        before :each do
            swarmpost('path/browse.mp4')
        end

        context 'when fragments are posted without error' do
            let (:post_response_1) {Net::HTTPSuccess.new 1.0, '200', 'failed'}

            it_behaves_like 'segment post agent'

            it 'generates and posts the playlist' do
                is_expected.to have_received(:post)
                .with('path/browse.m3u8',@exp_playlist, 'application/x-mpegurl',SwarmPost::RET_M3U8)
            end

            it "does not post other stuff" do
                is_expected.to have_received(:post)
                .exactly(5).times
            end
        end

        context 'when fragments are posted wit error' do
            let (:post_response_1) {Net::HTTPPreconditionFailed.new 1.0, '412', 'failed'}

            it_behaves_like 'segment post agent'

            it 'does not post the playlist' do
                is_expected.not_to have_received(:post)
                .with('path/browse.m3u8',@exp_playlist, 'application/x-mpegurl',SwarmPost::RET_M3U8)
            end

            it "does not post other stuff" do
                is_expected.to have_received(:post)
                .exactly(4).times
            end
        end

    end

    context 'when the m3u8 playlist exists' do
        let (:m3u8) { Net::HTTPSuccess.new 1.0, '200', 'OK' }
        let (:post_response_1) {Net::HTTPPreconditionFailed.new 1.0, '412', 'failed'}
        before :each do
            allow(m3u8).to receive(:body).and_return @exp_playlist
            swarmpost('path/browse.mp4')
        end

        it_behaves_like 'segment post agent'

        it 'does post the playlist' do
            is_expected.not_to have_received(:post)
            .with('path/browse.m3u8',@exp_playlist, 'application/x-mpegurl',SwarmPost::RET_M3U8)
        end

        it "does not post other stuff" do
            is_expected.to have_received(:post)
            .exactly(4).times
        end
    end

end
