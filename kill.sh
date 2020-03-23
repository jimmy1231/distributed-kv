#!/bin/sh

for PORT_NUM in 50007 50008 50009 50010 50011 50012 50013 50014 50015 50016 50017 50018 50019 50020 50021
do
    echo "killing ${PORT_NUM}"
    kill $(ps aux | grep ${PORT_NUM} | awk '{print $2}')
done

rm -f ~/kv_store* && rm -f ~/replica* && rm -f ~/logs/server.log