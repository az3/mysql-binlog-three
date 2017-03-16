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

    private static final String NULL_RECORD = "";
    private static final String COLUMN_SEPARATOR = "\t";
    private static final String RECORD_SEPARATOR = "\n"; // Not used.

    private volatile PrometheusHelper prometheus;
    private volatile long METRICS_LAST_TRACE_INFO_DIFF;

    public SampleRecordProcessor() {
        this.prometheus = new PrometheusHelper(Configuration.get(Configuration.schemaName, "test", String.class));
    }

    @Override
    public void onEvent(Event event) {
        try {
            EventHeaderV4 header = ((EventHeaderV4) event.getHeader());
            EventType eventType = header.getEventType();
            METRICS_LAST_TRACE_INFO_DIFF = System.currentTimeMillis() - header.getTimestamp();
            prometheus.gaugeDelay(METRICS_LAST_TRACE_INFO_DIFF);

            long scripttimer = System.nanoTime();
            if (eventType == EventType.EXT_WRITE_ROWS || eventType == EventType.WRITE_ROWS) {
                handleInsert(event);
            } else if (eventType == EventType.EXT_UPDATE_ROWS || eventType == EventType.UPDATE_ROWS) {
                handleUpdate(event);
            } else if (eventType == EventType.EXT_DELETE_ROWS || eventType == EventType.DELETE_ROWS) {
                handleDelete(event);
            }
            scripttimer = System.nanoTime() - scripttimer;
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
            String rowContent = parseRow(row);
            logger.info("Inserted Row: '{}'.", rowContent);
        }
    }

    private void handleUpdate(Event event) {
        UpdateRowsEventData data = ((UpdateRowsEventData) event.getData());
        List<Map.Entry<Serializable[], Serializable[]>> rows = data.getRows();
        for (Map.Entry<Serializable[], Serializable[]> entry : rows) {
            Serializable[] before = entry.getKey();
            String rowContent = parseRow(before);
            logger.info("Updated Row Before: '{}'.", rowContent);
            // FIXME maybe need to count before, too?
            // prometheus.histogramSize(before.length); 
            Serializable[] after = entry.getValue();
            prometheus.histogramSize(after.length);
            rowContent = parseRow(after);
            logger.info("Updated Row After: '{}'.", rowContent);
        }
    }

    private void handleDelete(Event event) {
        DeleteRowsEventData data = ((DeleteRowsEventData) event.getData());
        List<Serializable[]> rows = data.getRows();
        for (Serializable[] row : rows) {
            prometheus.histogramSize(row.length);
            String rowContent = parseRow(row);
            logger.info("Deleted Row: '{}'.", rowContent);
        }
    }

    private String parseRow(Serializable[] row) {
        StringBuilder oneLineRow = new StringBuilder();
        try {
            for (Serializable columnValue : row) {
                if (columnValue == null) {
                    oneLineRow.append(NULL_RECORD).append(COLUMN_SEPARATOR);
                } else {
                    oneLineRow.append(columnValue).append(COLUMN_SEPARATOR);
                }
            }
            oneLineRow.delete(oneLineRow.length() - 1, oneLineRow.length()); // removing column separator at the end.
            // oneLineRow.append(RECORD_SEPARATOR);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return oneLineRow.toString();
    }

}
