#!/bin/sh

for PORT_NUM in 50007 50008 50009
do
    echo "killing ${PORT_NUM}"
    kill $(ps aux | grep ${PORT_NUM} | awk '{print $2}')
done

