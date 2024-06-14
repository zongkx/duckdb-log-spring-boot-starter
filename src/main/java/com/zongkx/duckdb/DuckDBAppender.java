package com.zongkx.duckdb;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class DuckDBAppender extends AppenderBase<ILoggingEvent> implements SmartInitializingSingleton {
    private final DuckDBLog duckDBLog;

    @Override
    protected void append(ILoggingEvent eventObject) {
        duckDBLog.insert(eventObject.getFormattedMessage());
    }

    @Override
    public void afterSingletonsInstantiated() {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        this.setContext(context);
        this.start();
        Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.addAppender(this);
    }


}
