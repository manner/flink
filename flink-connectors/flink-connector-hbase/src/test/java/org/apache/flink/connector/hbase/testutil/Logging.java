package org.apache.flink.connector.hbase.testutil;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
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
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LOGGER_NAME);
        loggerConfig.setLevel(Level.ALL);
        ctx.updateLoggers();
    }
}
