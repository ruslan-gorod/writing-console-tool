package org.example.repository;

import lombok.AllArgsConstructor;
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
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Repository
@AllArgsConstructor
public class AppRepository {

    @Autowired
    private DataSource dataSource;
    private static final Random rand = new Random();

    public void initDatabase(AppParameters parameters) {
        try (Connection connection = dataSource.getConnection()) {
            dropAllTablesInDatabase(connection);
            createTables(parameters, connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void populateTables(AppParameters parameters) {
        getInfoAboutDatabase().forEach((key, value) -> populateTable(key, value, parameters));
    }

    private Map<String, List<TablesInfo>> getInfoAboutDatabase() {
        try (Statement stmt = dataSource.getConnection().createStatement()) {
            return getInfoMap(stmt, getTablesName(stmt));
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

    private String getInsertQuery(String table, List<TablesInfo> value, int countRows) {
        StringBuilder query = new StringBuilder("INSERT INTO " + table + " (");
        query.append(value.stream().map(TablesInfo::getColumnName).collect(Collectors.joining(", ")));
        query.append(") \nVALUES\n");
        for (int i = 0; i < countRows; i++) {
            query.append("\t(");
            query.append(getValuesToInsert(value));
            query.append(")");
            String commaOrSemicolon = i < countRows - 1 ? ",\n" : ";";
            query.append(commaOrSemicolon);
        }
        return query.toString();
    }

    private String getValuesToInsert(List<TablesInfo> tablesInfo) {
        return tablesInfo.stream().map(i -> getRandomValueByType(i.getDataType()))
                .collect(Collectors.joining(", "));
    }

    private String getRandomValueByType(String dataType) {
        String value;
        switch (dataType) {
            case "boolean" -> value = String.valueOf(rand.nextBoolean());
            case "integer" -> value = String.valueOf(rand.nextInt() + 100);
            case "double precision" -> value = String.valueOf(rand.nextDouble() + 100);
            case "character varying" -> value = "'" + generateRandomString(20) + "'";
            default -> throw new IllegalStateException("Unexpected type: " + dataType);
        }
        return value;
    }

    private static Map<String, List<TablesInfo>> getInfoMap(Statement stmt, List<String> tables) throws SQLException {
        Map<String, List<TablesInfo>> infoMap = new HashMap<>();
        for (String table : tables) {
            infoMap.put(table, getTablesInfo(stmt, table));
        }
        return infoMap;
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

    private static List<String> getTablesName(Statement stmt) throws SQLException {
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

    private void createTables(AppParameters parameters, Connection conn) {
        IntStream.rangeClosed(1, parameters.getCountTables()).parallel()
                .forEach(i -> createTableFromParameters(parameters, conn));
    }

    private void createTableFromParameters(AppParameters parameters, Connection conn) {
        String tableName = generateRandomString(parameters.getMaxNameLength());
        List<String> columns = generateRandomColumns(parameters);
        String createTableQuery = generateCreateTableQuery(tableName, columns);
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createTableQuery);
        } catch (SQLException e) {
            throw new RuntimeException("Error creating table " + tableName + ": " + e.getMessage(), e);
        }
    }

    private static void dropAllTablesInDatabase(Connection conn) {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("drop schema public cascade; create schema public;");
        } catch (SQLException e) {
            throw new RuntimeException("Error clear database: " + e.getMessage(), e);
        }
    }

    private String generateRandomString(int maxNameLength) {
        StringBuilder sb = new StringBuilder();
        int length = rand.nextInt(maxNameLength) + 10;
        IntStream.range(0, length).forEach(i -> {
            char c = (char) (rand.nextInt(26) + 'a');
            sb.append(c);
        });
        return sb.toString();
    }

    private List<String> generateRandomColumns(AppParameters parameters) {
        List<String> columns = new ArrayList<>();
        IntStream.rangeClosed(1, parameters.getCountColumns())
                .mapToObj(i -> parameters.getTypes())
                .forEach(types -> {
                    String columnName = generateRandomString(parameters.getMaxNameLength());
                    String columnType = types.get(rand.nextInt(types.size()));
                    String column = columnName + " " + columnType;
                    columns.add(column);
                });
        return columns;
    }

    private String generateCreateTableQuery(String tableName, List<String> columns) {
        return "CREATE TABLE " + tableName + " (" + String.join(", ", columns) + ")";
    }
}


