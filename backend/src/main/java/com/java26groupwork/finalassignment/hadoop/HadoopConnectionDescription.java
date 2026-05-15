package com.java26groupwork.finalassignment.hadoop;

public final class HadoopConnectionDescription {

    private final String mode;
    private final String fsDefaultFs;
    private final String nameservice;
    private final String yarnClusterId;
    private final String inputPath;
    private final String outputPath;
    private final String localBasePath;
    private final String configDir;

    public HadoopConnectionDescription(
            String mode,
            String fsDefaultFs,
            String nameservice,
            String yarnClusterId,
            String inputPath,
            String outputPath,
            String localBasePath,
            String configDir) {
        this.mode = mode;
        this.fsDefaultFs = fsDefaultFs;
        this.nameservice = nameservice;
        this.yarnClusterId = yarnClusterId;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.localBasePath = localBasePath;
        this.configDir = configDir;
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

    public String getLocalBasePath() {
        return localBasePath;
    }

    public String getConfigDir() {
        return configDir;
    }
}
