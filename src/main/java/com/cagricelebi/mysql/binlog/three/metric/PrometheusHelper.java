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

    private static final Histogram prometheusRecordHistogram = Histogram.build()
            .buckets(100d, 200d, 300d, 400d, 500d, 700d, 1000d, 5000d, 10000d, 50000d, 100000d)
            .name("binlog_records")
            .help("Tracks each binlog event passing though, and shows json event size in bytes.")
            .labelNames("schema_name").register();

    private static final Gauge prometheusDelayGauge = Gauge.build()
            .name("binlog_delay_milliseconds")
            .help("Picks a binlog event and calculates the difference between event timestamp and server timestamp in milliseconds.")
            .labelNames("schema_name").register();

    private static final Histogram prometheusProcessTimeHistogram = Histogram.build()
            .buckets(50d, 100d, 200d, 400d, 600d, 1000d, 10000d, 100000d)
            .name("binlog_event_process_time_nanoseconds")
            .help("Tracks each binlog event passing through, and shows how much time needed to process that single event in nanoseconds.")
            .labelNames("schema_name").register();

    public PrometheusHelper(String schemaName) {
        this.schemaName = schemaName;
    }

    public void histogramSize(double bytes) {
        try {
            prometheusRecordHistogram.labels(schemaName).observe(bytes);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void gaugeDelay(long millis) {
        try {
            prometheusDelayGauge.labels(schemaName).set(millis);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void gaugeProcessTime(long nanos) {
        try {
            prometheusProcessTimeHistogram.labels(schemaName).observe(nanos);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void shutdown() {
        prometheusRecordHistogram.clear();
        prometheusDelayGauge.clear();
        prometheusProcessTimeHistogram.clear();
    }
}
