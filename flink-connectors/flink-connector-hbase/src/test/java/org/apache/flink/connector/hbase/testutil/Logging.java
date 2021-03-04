package org.apache.flink.connector.hbase.testutil;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides common logger for all hbase connector tests. */
public class Logging {

    private static final String LOGGER_NAME = "[HBase Connector Tests]";
    public static final Logger LOG = LoggerFactory.getLogger(LOGGER_NAME);

    static {
        enableLogging();
    }

    private static void enableLogging() {
        Configurator.setLevel(LOGGER_NAME, Level.ALL);
    }
}
