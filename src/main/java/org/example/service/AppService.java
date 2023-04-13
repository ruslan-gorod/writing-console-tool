package org.example.service;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.example.model.AppParameters;
import org.example.repository.AppRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

@Service
@AllArgsConstructor
@NoArgsConstructor
public class AppService {

    @Autowired
    private AppRepository repository;

    public void runTask(String[] args) {
        AppParameters parameters = getParametersFromArgs(args);
        repository.initDatabase(parameters);
        repository.populateTables(parameters);
    }

    private AppParameters getParametersFromArgs(String[] args) {
        return getAppParameters(getParsedDataFromFile(args));
    }

    private static List<String> getParsedDataFromFile(String[] args) {
        String filePath = args[0];
        try {
            return Files.readAllLines(Paths.get(filePath));
        } catch (IOException e) {
            throw new RuntimeException("Can't read file: " + filePath, e);
        }
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
                case "connections" -> parameters.setConnections(Integer.parseInt(value));
                case "maxNameLength" -> parameters.setMaxNameLength(Integer.parseInt(value));
                case "types" -> parameters.setTypes(Arrays.asList(value.split(",")));
                default -> throw new RuntimeException("Unknown field:" + key);
            }
        }
        return parameters;
    }
}
