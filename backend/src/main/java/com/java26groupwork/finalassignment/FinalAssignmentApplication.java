package com.java26groupwork.finalassignment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class FinalAssignmentApplication {

    public static void main(String[] args) {
        SpringApplication.run(FinalAssignmentApplication.class, args);
    }
}
