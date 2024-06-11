package com.zongkx.duckdb;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.duckdb.DuckDBDriver;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;

@Configuration
@RestController
@RequestMapping("/logs")
@Slf4j
public class DuckDBAppender extends AppenderBase<ILoggingEvent> implements SmartInitializingSingleton {
    private DataSource dataSource;


    @GetMapping
    public Object get() {
        log.info("aaaaa");
        return DuckDBUtil.select();
    }

    @Bean("duckdbDataSource")
    public DataSource dataSource() {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(DuckDBDriver.class.getCanonicalName());
        config.setMaximumPoolSize(10);
        config.setJdbcUrl("jdbc:duckdb:");
        dataSource = new HikariDataSource(config);
        return dataSource;
    }

    @Bean
    public DuckDBUtil duckDBUtil() {
        return new DuckDBUtil(dataSource);
    }

    @Override
    protected void append(ILoggingEvent eventObject) {
        DuckDBUtil.insert(eventObject.getFormattedMessage());
    }

    @Override
    public void afterSingletonsInstantiated() {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        DuckDBAppender customAppender = new DuckDBAppender();
        customAppender.setContext(context);
        customAppender.start();
        Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.addAppender(customAppender);
    }


}
