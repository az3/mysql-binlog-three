#/usr/bin/env bash
while true; do
    echo `date` >>prometheus.log
    curl -s localhost:8091/metrics >>prometheus.log
    sleep 60
done
