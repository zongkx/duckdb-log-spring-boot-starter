package com.zongkx.duckdb;

import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({DuckDBApi.class, DuckDBAppender.class, DuckDBLog.class})
@AutoConfigureOrder()
public class DuckDBAutoConfiguration {

}
