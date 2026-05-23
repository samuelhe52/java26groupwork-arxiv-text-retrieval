package com.java26groupwork.finalassignment.hadoop;

import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(HadoopProperties.class)
public class HadoopConfig {

    private static final Logger log = LoggerFactory.getLogger(HadoopConfig.class);

    @Bean
    public org.apache.hadoop.conf.Configuration hadoopConfiguration(HadoopProperties properties) {
        if (properties.getMode() == HadoopProperties.Mode.LOCAL) {
            return new org.apache.hadoop.conf.Configuration(false);
        }

        // Keep Hadoop's built-in defaults so client-side abstractions like FileContext
        // still know the core filesystem implementations for schemes such as hdfs.
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();

        Path configDir = Path.of(properties.getConfigDir());
        addRequiredResource(configuration, configDir.resolve("core-site.xml"));
        addRequiredResource(configuration, configDir.resolve("hdfs-site.xml"));
        addRequiredResource(configuration, configDir.resolve("mapred-site.xml"));

        Path yarnSite = configDir.resolve("yarn-site.xml");
        if (Files.isReadable(yarnSite)) {
            configuration.addResource(new org.apache.hadoop.fs.Path(yarnSite.toUri()));
        }

        configuration.setInt("dfs.replication", Math.max(1, properties.getReplicationFactor()));
        configuration.setInt(
                "mapreduce.client.submit.file.replication",
                Math.max(1, properties.getReplicationFactor()));

        log.info(
                "cluster mode enabled: hdfsAddress={} hadoopConfDir={}",
                configuration.getTrimmed("fs.defaultFS", "unknown"),
                configDir);

        return configuration;
    }

    private static void addRequiredResource(
            org.apache.hadoop.conf.Configuration configuration, Path filePath) {
        if (!Files.isReadable(filePath)) {
            throw new IllegalStateException("Required Hadoop config file is not readable: " + filePath);
        }
        configuration.addResource(new org.apache.hadoop.fs.Path(filePath.toUri()));
    }
}
