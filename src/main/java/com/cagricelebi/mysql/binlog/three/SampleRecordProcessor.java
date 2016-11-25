package com.cagricelebi.mysql.binlog.three;

import com.cagricelebi.mysql.binlog.three.metric.PrometheusHelper;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cagricelebi
 */
public class SampleRecordProcessor implements BinaryLogClient.EventListener {

    private static final Logger logger = LoggerFactory.getLogger(SampleRecordProcessor.class);

    private volatile PrometheusHelper prometheus;

    private volatile long METRICS_LAST_TRACE_INFO_DIFF;

    public SampleRecordProcessor() {
        this.prometheus = new PrometheusHelper(Configuration.get(Configuration.schemaName, "test", String.class));
    }

    @Override
    public void onEvent(Event event) {
        try {
            long scripttimer = System.currentTimeMillis();

            EventHeaderV4 header = ((EventHeaderV4) event.getHeader());
            EventType eventType = header.getEventType();

            METRICS_LAST_TRACE_INFO_DIFF = System.currentTimeMillis() + header.getTimestamp();
            prometheus.gaugeDelay(METRICS_LAST_TRACE_INFO_DIFF);

            if (eventType == EventType.EXT_WRITE_ROWS || eventType == EventType.WRITE_ROWS) {
                handleInsert(event);
            } else if (eventType == EventType.EXT_UPDATE_ROWS || eventType == EventType.UPDATE_ROWS) {
                handleUpdate(event);
            } else if (eventType == EventType.EXT_DELETE_ROWS || eventType == EventType.DELETE_ROWS) {
                handleDelete(event);
            }

            scripttimer = System.currentTimeMillis() - scripttimer;
            prometheus.gaugeProcessTime(scripttimer);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void handleInsert(Event event) {
        WriteRowsEventData data = ((WriteRowsEventData) event.getData());
        List<Serializable[]> rows = data.getRows();
        for (Serializable[] row : rows) {
            prometheus.histogramSize(row.length);
        }
    }

    private void handleUpdate(Event event) {
        UpdateRowsEventData data = ((UpdateRowsEventData) event.getData());
        List<Map.Entry<Serializable[], Serializable[]>> rows = data.getRows();
        for (Map.Entry<Serializable[], Serializable[]> entry : rows) {
            Serializable[] before = entry.getKey();
            prometheus.histogramSize(before.length);
            Serializable[] after = entry.getValue();
            prometheus.histogramSize(after.length);
        }
    }

    private void handleDelete(Event event) {
        DeleteRowsEventData data = ((DeleteRowsEventData) event.getData());
        List<Serializable[]> rows = data.getRows();
        for (Serializable[] row : rows) {
            prometheus.histogramSize(row.length);
        }
    }

}
