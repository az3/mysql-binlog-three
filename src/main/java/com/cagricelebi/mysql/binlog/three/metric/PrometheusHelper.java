package com.cagricelebi.mysql.binlog.three.metric;

import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cagricelebi
 */
public class PrometheusHelper {

    private static final Logger logger = LoggerFactory.getLogger(PrometheusHelper.class);

    private final String schemaName;

    private static final Histogram prometheusHistogram = Histogram.build()
            .buckets(100d, 200d, 300d, 400d, 500d, 700d, 1000d, 5000d, 10000d, 50000d, 100000d)
            .name("binlog_records")
            .help("Tracks each binlog event passing though, and shows json event size in bytes.")
            .labelNames("schema_name").register();

    private static final Gauge prometheusDelayGauge = Gauge.build()
            .name("binlog_delay_milliseconds")
            .help("Picks a binlog event and calculates the difference between event timestamp and server timestamp in milliseconds.")
            .labelNames("schema_name").register();

    private static final Gauge prometheusProcessTimeGauge = Gauge.build()
            .name("binlog_process_time_milliseconds")
            .help("Tracks each binlog event passing through, and shows how much time needed to process that single event.")
            .labelNames("schema_name").register();

    public PrometheusHelper(String schemaName) {
        this.schemaName = schemaName;
    }

    public void histogramSize(double recSize) {
        try {
            prometheusHistogram.labels(schemaName).observe(recSize);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void gaugeDelay(long diff) {
        try {
            prometheusDelayGauge.labels(schemaName).set(diff);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void gaugeProcessTime(long scripttimer) {
        try {
            prometheusProcessTimeGauge.labels(schemaName).set(scripttimer);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void shutdown() {
        prometheusHistogram.clear();
        prometheusDelayGauge.clear();
        prometheusProcessTimeGauge.clear();
    }
}
