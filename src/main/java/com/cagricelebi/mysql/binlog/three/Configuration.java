package com.cagricelebi.mysql.binlog.three;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads properties file defined with CONFIG_FILE parameter.
 *
 * <pre>
 * java -DCONFIG_FILE=prod.properties -jar app.jar
 * </pre>
 *
 * @author cagricelebi
 */
public class Configuration {

    private static final Logger logger = LoggerFactory.getLogger(Configuration.class);
    private final Properties props;

    // BEGIN configurable variables.
    public static final String prometheusPort = "prometheusPort";

    public static final String schemaName = "schemaName";

    public static final String jdbcHost = "jdbcHost";
    public static final String jdbcPort = "jdbcPort";
    public static final String jdbcUser = "jdbcUser";
    public static final String jdbcPass = "jdbcPass";

    public static final String binlogStartFile = "binlogStartFile";
    public static final String binlogStartPosition = "binlogStartPosition";
    public static final String binlogEndFile = "binlogEndFile";
    public static final String binlogEndPosition = "binlogEndPosition";
    // END configurable variables.

    // <editor-fold defaultstate="collapsed" desc="Singleton structure.">
    private Configuration() {
        props = new Properties();
    }

    private static class ConfigurationInstanceHolder {

        private static final Configuration instance = new Configuration();
    }

    private static synchronized Configuration getInstance() {
        return ConfigurationInstanceHolder.instance;
    }
    // </editor-fold>

    public static void init() {
        try {
            logger.info("(read) Started reading config.");
            String path = System.getProperty("CONFIG_FILE", "config.properties");
            logger.info("(read) Configuration path: " + path);

            try (InputStream configStream = new FileInputStream(path)) {
                getInstance().props.load(configStream);
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static <T> T get(String key, T defaultValue, Class<T> type) {
        String val = getInstance().props.getProperty(key, String.valueOf(defaultValue));
        Object ret;

        if (type == Long.class) {
            ret = Long.parseLong(val);
        } else if (type == Integer.class) {
            ret = Integer.parseInt(val);
        } else if (type == Boolean.class) {
            ret = Boolean.parseBoolean(val);
        } else if (type == String.class) {
            ret = val;
        } else if (type == Float.class) {
            ret = Float.parseFloat(val);
        } else if (type == Double.class) {
            ret = Double.parseDouble(val);
        } else {
            ret = getInstance().props.getOrDefault(key, defaultValue);
        }

        return type.cast(ret);
    }

}
