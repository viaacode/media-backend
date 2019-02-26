require 'resolv'

class Bus
  @@dns = Resolv::DNS.new( nameserver: [ '10.50.104.62'])

  attr_reader :hosts

  def resolve
    host, port = @host&.split(':')
    port = port&.to_i || 11300
    hosts = @hosts
    begin
      @hosts = [ { host: Resolv::IPv4.create(host).to_s, port: port} ]
    rescue ArgumentError
      begin
        servicehosts = @@dns.getresources("_beanstalk._tcp.#{host}", Resolv::DNS::Resource::IN::SRV)
        raise RuntimeError if servicehosts&.empty?
        @hosts = servicehosts&.map { |host| { host: host.target.to_s, port: host.port } }
      rescue RuntimeError
        @hosts = @@dns.getaddresses(host)&.map { | host| { host: host.to_s, port: port } }
      end
    end
    raise RuntimeError if @hosts.empty?
    @hosts.sort! { |a,b| a[:host] <=> b[:host] }
    return @hosts != hosts
  end

  def initialize(host,queues)
    @host = host
    @queues = Array queues
    @beanstalk = {}
  end

  def process
      threads = {}
      while true do
        resolve
        @hosts.each do |host|
          threads[host] ||= Thread.new do
            begin
            beanstalk = Beaneater.new "#{host[:host]}:#{host[:port]}"
            @beanstalk[host] = beanstalk
            begin
              # beanstalk connection is autohealing, when the beanstalk daemon it is connected to
              # is restarted, but it looses watched tubes
              watched_queue_names = beanstalk.tubes.watched.map(&:name)
              @queues.each { |q| beanstalk.tubes.watch(q) unless watched_queue_names.include?(q) }
              while job = beanstalk.tubes.reserve(5) do
                yield(job)
              end
            rescue Beaneater::TimedOutError
               retry
            end
            rescue Beaneater::NotConnected 
                puts "error connecting to #{host}"
            ensure
                puts "closing thread for #{host[:host]}"
                beanstalk&.close
                @beanstalk.delete(host)
            end
          end
        end
        sleep 10
        threads.delete_if { |k,t| ! t.status }
      end
      puts "exit, #{@host}"
      threads.values.each { |t| t.join }
  end
end
