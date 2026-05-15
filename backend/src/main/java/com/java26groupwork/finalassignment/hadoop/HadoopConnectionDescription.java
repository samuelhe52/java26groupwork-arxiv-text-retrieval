package com.java26groupwork.finalassignment.hadoop;

public final class HadoopConnectionDescription {

    private final String mode;
    private final String fsDefaultFs;
    private final String nameservice;
    private final String yarnClusterId;
    private final String inputPath;
    private final String outputPath;
    private final String configDir;
    private final int reducerTasks;

    public HadoopConnectionDescription(
            String mode,
            String fsDefaultFs,
            String nameservice,
            String yarnClusterId,
            String inputPath,
            String outputPath,
            String configDir,
            int reducerTasks) {
        this.mode = mode;
        this.fsDefaultFs = fsDefaultFs;
        this.nameservice = nameservice;
        this.yarnClusterId = yarnClusterId;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.configDir = configDir;
        this.reducerTasks = reducerTasks;
    }

    public String getMode() {
        return mode;
    }

    public String getFsDefaultFs() {
        return fsDefaultFs;
    }

    public String getNameservice() {
        return nameservice;
    }

    public String getYarnClusterId() {
        return yarnClusterId;
    }

    public String getInputPath() {
        return inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public String getConfigDir() {
        return configDir;
    }

    public int getReducerTasks() {
        return reducerTasks;
    }
}
