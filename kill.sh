#!/bin/sh

for PORT_NUM in 50000 50001 50002
do
    echo "killing ${PORT_NUM}"
    kill $(ps aux | grep ${PORT_NUM} | awk '{print $2}')
done

