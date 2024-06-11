package com.zongkx.duckdb;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DuckDBUtil {
    public static DataSource dataSource;

    public DuckDBUtil(@Qualifier("duckdbDataSource") DataSource dataSource) {
        DuckDBUtil.dataSource = dataSource;
        create();
    }

    public static void create() {
        try (Connection connection = dataSource.getConnection()) {
            Statement stmt = connection.createStatement();
            stmt.execute("create table memory.main.t_logs( content text)");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Object select() {
        List<Map<String, Object>> result = new ArrayList<>();
        try (Connection connection = dataSource.getConnection()) {
            Statement stmt = connection.createStatement();
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM memory.main.t_logs")) {
                while (rs.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("content", rs.getString(1));
                    result.add(map);
                }
            }

        } catch (Exception e) {
        }
        return result;
    }

    public static void insert(String content) {
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO memory.main.t_logs values (?);")) {
                stmt.setString(1, content);
                stmt.execute();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
