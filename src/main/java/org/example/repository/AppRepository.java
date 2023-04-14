package org.example.repository;

import lombok.AllArgsConstructor;
import org.example.helper.Helper;
import org.example.model.AppParameters;
import org.example.model.TablesInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Repository
@AllArgsConstructor
public class AppRepository {

    @Autowired
    private DataSource dataSource;

    public void initDatabase(AppParameters parameters) {
        try (Connection connection = dataSource.getConnection()) {
            dropAllTablesInDatabase(connection);
            createTables(parameters, connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void populateTables(AppParameters parameters) {
        Map<String, List<TablesInfo>> map = getInfoAboutDatabase();
        map.keySet().stream().parallel().forEach(k -> populateTable(k, map.get(k), parameters));
    }

    private void dropAllTablesInDatabase(Connection conn) {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("drop schema public cascade; create schema public;");
        } catch (SQLException e) {
            throw new RuntimeException("Error clear database: " + e.getMessage(), e);
        }
    }

    private void createTables(AppParameters parameters, Connection conn) {
        IntStream.rangeClosed(1, parameters.getCountTables()).parallel()
                .forEach(i -> createTableFromParameters(parameters, conn));
    }

    private Map<String, List<TablesInfo>> getInfoAboutDatabase() {
        try (Statement stmt = dataSource.getConnection().createStatement()) {
            return getInfoMap(stmt, getTableNames(stmt));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void populateTable(String table, List<TablesInfo> value, AppParameters parameters) {
        try (Statement stmt = dataSource.getConnection().createStatement()) {
            stmt.execute(getInsertQuery(table, value, parameters.getCountRows()));
        } catch (SQLException e) {
            throw new RuntimeException("Table: " + table, e);
        }
    }

    private Map<String, List<TablesInfo>> getInfoMap(Statement stmt, List<String> tables) throws SQLException {
        Map<String, List<TablesInfo>> infoMap = new HashMap<>();
        for (String table : tables) {
            infoMap.put(table, getTablesInfo(stmt, table));
        }
        return infoMap;
    }

    private static List<String> getTableNames(Statement stmt) throws SQLException {
        ResultSet resultSet = stmt.executeQuery("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public';""");
        List<String> tables = new ArrayList<>();
        while ((resultSet.next())) {
            tables.add(resultSet.getString(1));
        }
        return tables;
    }

    private String getInsertQuery(String table, List<TablesInfo> value, int countRows) {
        StringBuilder query = new StringBuilder("INSERT INTO " + table + " (");
        query.append(value.stream().map(TablesInfo::getColumnName).collect(Collectors.joining(", ")));
        query.append(") \nVALUES\n");
        for (int i = 0; i < countRows; i++) {
            query.append("\t(");
            query.append(Helper.getRandomValuesToInsert(value));
            query.append(")");
            String commaOrSemicolon = i < countRows - 1 ? ",\n" : ";";
            query.append(commaOrSemicolon);
        }
        return query.toString();
    }

    private static List<TablesInfo> getTablesInfo(Statement stmt, String table) throws SQLException {
        ResultSet resultSet = stmt.executeQuery("SELECT column_name, data_type\n" +
                "FROM information_schema.columns\n" +
                "WHERE table_name = '" + table + "';");
        List<TablesInfo> tablesInfoList = new ArrayList<>();
        while (resultSet.next()) {
            tablesInfoList.add(new TablesInfo(
                    resultSet.getString(1),
                    resultSet.getString(2)));
        }
        return tablesInfoList;
    }

    private void createTableFromParameters(AppParameters parameters, Connection conn) {
        String tableName = Helper.generateRandomString(parameters.getMaxNameLength());
        List<String> columns = Helper.generateRandomColumns(parameters);
        String createTableQuery = generateCreateTableQuery(tableName, columns);
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createTableQuery);
        } catch (SQLException e) {
            throw new RuntimeException("Error creating table " + tableName + ": " + e.getMessage(), e);
        }
    }

    private String generateCreateTableQuery(String tableName, List<String> columns) {
        return "CREATE TABLE " + tableName + " (" + String.join(", ", columns) + ")";
    }
}


