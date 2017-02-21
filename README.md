# media-backend

* Listens on a [beanstalk](https://kr.github.io/beanstalkd/) queue for jobs
* creates segmented mpegts files and uploads them to a caringo swarm object store

###Requires
* [viaacode/mpegts-segmenter](https://github.com/viaacode/mpegts-segmenter)
* [viaacode/swarmbucket](https://github.com/viaacode/swarmbucket)

###Usage:

Manual startup
```
bundle install
ruby dispatch.rb
```

Using systemd
```
[Unit]
Description=media mpegts segmenter and uploader

[Service]
Type=simple
User=media
Group=media
WorkingDirectory=/home/media/media_backend
ExecStart=/home/media/.rvm/wrappers/ruby-2.3.0/ruby /home/media/media_backend/dispatch.rb
Restart=always
KillSignal=SIGTERM
StandardInput=null
```

