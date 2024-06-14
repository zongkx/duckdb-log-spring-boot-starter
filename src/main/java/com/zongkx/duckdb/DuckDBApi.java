package com.zongkx.duckdb;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/logs")
@RequiredArgsConstructor
@Slf4j
public class DuckDBApi {
    private final DuckDBLog duckDBLog;

    @GetMapping
    @SneakyThrows
    public Object get() {
        log.info("aaaaa");
        return duckDBLog.select();
    }
}
