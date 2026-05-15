package com.java26groupwork.finalassignment.hadoop;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.hadoop")
public class HadoopProperties {

    private Mode mode = Mode.LOCAL;
    private String configDir;
    private String inputPath;
    private String outputPath;
    private int reducerTasks = 4;

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public String getConfigDir() {
        return configDir;
    }

    public void setConfigDir(String configDir) {
        this.configDir = configDir;
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public int getReducerTasks() {
        return reducerTasks;
    }

    public void setReducerTasks(int reducerTasks) {
        this.reducerTasks = reducerTasks;
    }

    public enum Mode {
        LOCAL,
        CLUSTER
    }
}
