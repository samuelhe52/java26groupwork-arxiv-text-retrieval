package com.java26groupwork.finalassignment.hadoop;

import java.nio.file.Files;
import java.nio.file.Path;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(HadoopProperties.class)
public class HadoopConfig {

    @Bean
    public org.apache.hadoop.conf.Configuration hadoopConfiguration(HadoopProperties properties) {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration(false);

        if (properties.getMode() == HadoopProperties.Mode.LOCAL) {
            configuration.set("fs.defaultFS", "file:///");
            configuration.set("mapreduce.framework.name", "local");
            configuration.set("fs.file.impl",
                    LocalDevFileSystem.class.getName());
            configuration.set("fs.file.impl.disable.cache", "true");
            return configuration;
        }

        Path configDir = Path.of(properties.getConfigDir());
        addRequiredResource(configuration, configDir.resolve("core-site.xml"));
        addRequiredResource(configuration, configDir.resolve("hdfs-site.xml"));

        Path yarnSite = configDir.resolve("yarn-site.xml");
        if (Files.isReadable(yarnSite)) {
            configuration.addResource(new org.apache.hadoop.fs.Path(yarnSite.toUri()));
        }

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
