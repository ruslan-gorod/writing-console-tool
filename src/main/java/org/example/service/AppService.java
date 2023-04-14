package org.example.service;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.example.helper.Helper;
import org.example.model.AppParameters;
import org.example.repository.AppRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
@NoArgsConstructor
public class AppService {

    @Autowired
    private AppRepository repository;

    public void runTask(String[] args) {
        AppParameters parameters = Helper.getParametersFromArgs(args);
        repository.initDatabase(parameters);
        repository.populateTables(parameters);
    }
}
