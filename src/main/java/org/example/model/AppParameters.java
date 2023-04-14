package org.example.model;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class AppParameters {
    private int countTables;
    private int countColumns;
    private int countRows;
    private int maxNameLength;
    private List<String> types;
}
