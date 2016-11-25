#/usr/bin/env bash
echo -n "" >prometheus.log
while true; do
    echo `date` >>prometheus.log
    curl -s localhost:8091/metrics >>prometheus.log
    sleep 1
done
