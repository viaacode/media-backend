#!/usr/bin/env ruby
#
# Listens on a beanstalk queue and dispatches for mpegts conversion jobs
#
#require 'bundler/setup'
require 'beaneater'
require_relative 'tsbucket'
require_relative 'peakbucket'


# disable buffering of output so that activity can be followed
# in real time when run by systemd
STDOUT.sync = true

config = YAML.load_file 'config.yaml'
redis = Redis.new host: config['redishost']
hosts = Array config['beanstalkhost']
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
    beanstalk = Beaneater.new beanstalkhost
    beanstalk.jobs.register(config['mpegts']['queue']) do |job| 
        tsbucket.create(job.body)
    end
    beanstalk.jobs.register(config['peak']['queue']) do |job| 
        peakbucket.create(job.body)
    end
    threads << Thread.new do 
        beanstalk.jobs.process!
    end
end
threads.each { |t| t.join }

