#!/usr/bin/env ruby
#
# Listens on a beanstalk queue and dispatches for mpegts conversion jobs
#
#require 'bundler/setup'
require 'beaneater'
require_relative 'bus'
require_relative 'tsbucket'
require_relative 'peakbucket'

# disable buffering of output so that activity can be followed
# in real time when run by systemd
STDOUT.sync = true


config = YAML.load_file 'config.yaml'
redis = Redis.new host: config['redishost']
hosts = Array config['beanstalkhost']
srvhosts = Array config['beanstalksrv']
threads = []

tsbucket = TsBucket.new domain: config['url'],
  bucket: config['mpegts']['bucket'],
  source_path:  "http://#{config['url']}/#{config['sourcebucket']}",
  redis: redis

peakbucket = PeakBucket.new domain: config['url'],
    bucket: config['peak']['bucket'],
    username: config['peak']['user'],
    password: config['peak']['password'],
    source_path:  "http://#{config['url']}/#{config['sourcebucket']}",
    redis: redis

hosts.each do |beanstalkhost|
  threads << Thread.new do
      mpegts = Bus.new(beanstalkhost,[ config['mpegts']['queue'], config['peak']['queue'] ])
      mpegts.process do |job|
        puts :job, job, job.tube
        tsbucket.create(job.body) if job.tube == config['mpegts']['queue']
        peakbucket.create(job.body) if job.tube == config['peak']['queue']
        job.delete
    end
  end
end
threads.each{ |t| t.join }
