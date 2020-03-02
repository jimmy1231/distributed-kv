#!/bin/sh
host=$1
port=$2
ssh -n ${host} nohup java -jar "~/Desktop/ECE419/ECE419-Distributed-System/m2-server.jar" ${port} 256 FIFO &
