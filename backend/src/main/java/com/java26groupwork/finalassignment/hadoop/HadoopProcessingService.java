package com.java26groupwork.finalassignment.hadoop;

import org.springframework.stereotype.Service;

@Service
public class HadoopProcessingService {

    private final HadoopProperties properties;
    private final org.apache.hadoop.conf.Configuration hadoopConfiguration;

    public HadoopProcessingService(
            HadoopProperties properties, org.apache.hadoop.conf.Configuration hadoopConfiguration) {
        this.properties = properties;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    public String describe() {
        return properties.getMode() == HadoopProperties.Mode.LOCAL
                ? "local-filesystem"
                : "hdfs-yarn-cluster";
    }

    public HadoopConnectionDescription describeConnection() {
        return new HadoopConnectionDescription(
                properties.getMode().name().toLowerCase(),
                hadoopConfiguration.get("fs.defaultFS"),
                hadoopConfiguration.get("dfs.nameservices"),
                hadoopConfiguration.get("yarn.resourcemanager.cluster-id"),
                properties.getInputPath(),
                properties.getOutputPath(),
                properties.getLocalBasePath(),
                properties.getConfigDir());
    }
}
