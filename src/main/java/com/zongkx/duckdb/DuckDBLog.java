package com.zongkx.duckdb;

import jakarta.annotation.PostConstruct;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Configuration
@Slf4j
public class DuckDBLog {
    public Connection connection;

    @SneakyThrows
    @PostConstruct
    public void init() {
        connection = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = connection.createStatement();
        stmt.execute("create table memory.main.t_logs( content text)");
        log.info("duckdb appender init success ");
    }

    @SneakyThrows
    public Object select() {
        List<Map<String, Object>> result = new ArrayList<>();
        Statement stmt = connection.createStatement();
        try (ResultSet rs = stmt.executeQuery("SELECT * FROM memory.main.t_logs")) {
            while (rs.next()) {
                Map<String, Object> map = new HashMap<>();
                map.put("content", rs.getString(1));
                result.add(map);
            }
        }
        return result;
    }

    @SneakyThrows
    public void insert(String content) {
        CompletableFuture.runAsync(() -> {
            try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO memory.main.t_logs values (?);")) {
                stmt.setString(1, content);
                stmt.execute();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
