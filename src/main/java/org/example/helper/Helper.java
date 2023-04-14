package org.example.helper;

import org.example.model.AppParameters;
import org.example.model.TablesInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Helper {

    private static final Random rand = new Random();

    public static AppParameters getParametersFromArgs(String[] args) {
        return getAppParameters(getParsedDataFromFile(args));
    }

    public static String generateRandomString(int maxNameLength) {
        StringBuilder sb = new StringBuilder();
        int length = rand.nextInt(maxNameLength) + 10;
        IntStream.range(0, length).forEach(i -> {
            char c = (char) (rand.nextInt(26) + 'a');
            sb.append(c);
        });
        return sb.toString();
    }

    public static List<String> generateRandomColumns(AppParameters parameters) {
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

    public static String getRandomValuesToInsert(List<TablesInfo> tablesInfo) {
        return tablesInfo.stream().map(i -> getRandomValueByType(i.getDataType()))
                .collect(Collectors.joining(", "));
    }

    private static AppParameters getAppParameters(List<String> lines) {
        AppParameters parameters = new AppParameters();
        for (String str : lines) {
            String[] keyValue = str.trim().split("=");
            String key = keyValue[0];
            String value = keyValue[1];
            switch (key) {
                case "countTables" -> parameters.setCountTables(Integer.parseInt(value));
                case "countColumns" -> parameters.setCountColumns(Integer.parseInt(value));
                case "countRows" -> parameters.setCountRows(Integer.parseInt(value));
                case "maxNameLength" -> parameters.setMaxNameLength(Integer.parseInt(value));
                case "types" -> parameters.setTypes(Arrays.asList(value.split(",")));
                default -> throw new RuntimeException("Unknown field:" + key);
            }
        }
        return parameters;
    }

    private static List<String> getParsedDataFromFile(String[] args) {
        String filePath = args[0];
        try {
            return Files.readAllLines(Paths.get(filePath));
        } catch (IOException e) {
            throw new RuntimeException("Can't read file: " + filePath, e);
        }
    }

    private static String getRandomValueByType(String dataType) {
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
}
