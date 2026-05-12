package com.java26groupwork.finalassignment.hadoop;

public record HadoopConnectionDescription(
        String mode,
        String fsDefaultFs,
        String nameservice,
        String yarnClusterId,
        String inputPath,
        String outputPath,
        String localBasePath,
        String configDir) {}
