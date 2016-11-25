package com.cagricelebi.mysql.binlog.three;

import com.cagricelebi.mysql.binlog.three.metric.PrometheusRunnable;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cagricelebi
 */
public class Starter {

    private static final Logger logger = LoggerFactory.getLogger(Starter.class);

    private ExecutorService prometheusExecutor;
    private String prometheusShutdownKey; // shutdown key can be used for graceful shutdown, not implemented.

    public static void main(String[] args) {
        try {
            Starter s = new Starter();
            s.start();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void start() {
        try {
            Configuration.init();
            initPrometheus();
            initBinlog();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void initBinlog() throws IOException {

        BinaryLogClient client = new BinaryLogClient(
                Configuration.get(Configuration.jdbcHost, "", String.class),
                Configuration.get(Configuration.jdbcPort, 3306, Integer.class),
                Configuration.get(Configuration.jdbcUser, "", String.class),
                Configuration.get(Configuration.jdbcPass, "", String.class));
        client.setKeepAlive(true);

        try {
            client.setServerId((long) new Random().nextInt(65534));
        } catch (Exception e1) {
            logger.error(e1.getMessage(), e1);
        }

        client.setBlocking(true);
        client.setKeepAliveConnectTimeout(10000L);
        client.setKeepAliveInterval(10000L);

        String fileName = Configuration.get(Configuration.binlogStartFile, "", String.class);
        long pos = Configuration.get(Configuration.binlogStartPosition, 0L, Long.class);

        if (pos > 0L && fileName != null && !fileName.isEmpty()) {
            client.setBinlogFilename(fileName);
            client.setBinlogPosition(pos);
        }
        client.registerEventListener(new SampleRecordProcessor());
        client.connect();
    }

    private void initPrometheus() {
        int port = Configuration.get(Configuration.prometheusPort, 8091, Integer.class);
        prometheusExecutor = Executors.newSingleThreadExecutor();
        prometheusShutdownKey = new BigInteger(130, new SecureRandom()).toString(32);
        logger.info("Prometheus on port {} with shutdownKey: '{}'", port, prometheusShutdownKey);
        prometheusExecutor.submit(new PrometheusRunnable(prometheusShutdownKey, port));
    }

}
