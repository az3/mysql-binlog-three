#/usr/bin/env bash
nohup java -server -XX:+UseG1GC -DCONFIG_FILE=/data/binlog/prod.properties -jar /data/binlog/mysql-binlog-three.jar >>/data/kinesis/out.log 2>&1 &
