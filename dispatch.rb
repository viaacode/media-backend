#!/usr/bin/env ruby
#
# Listens on a beanstalk queue and dispatches for mpegts conversion jobs
#
#require 'bundler/setup'
require 'beaneater'
require_relative 'swarmpost'
require_relative 'swarmpeak'

include SwarmPost
include SwarmPeak

# disable buffering of output so that activity can be followed
# in real time when run by systemd
STDOUT.sync = true

config = YAML.load_file 'config.yaml'
queue = config['beanstalkqueue']
hosts = Array config['beanstalkhost']
threads = []

SWARMHTTP = SwarmBucket.new domain: config['swarmurl'], bucket: config['tsbucket']
SOURCEPATH = "http://#{config['swarmurl']}/#{config['sourcebucket']}"
REDIS = Redis.new  host: config['redishost']
PEAK_BUCKET = SwarmBucket.new domain: config['swarmurl'], bucket: config['sourcebucket'], username: config['swarmuser'], password: config['swarmpassword']

hosts.each do |beanstalkhost|
    beanstalk = Beaneater.new beanstalkhost
    beanstalk.jobs.register(queue) do |job| 
        swarmpost(job.body)
    end
    beanstalk.jobs.register('peak') do |job| 
        swarmpeak(job.body)
    end
    threads << Thread.new do 
        beanstalk.jobs.process!
    end
end
threads.each { |t| t.join }

