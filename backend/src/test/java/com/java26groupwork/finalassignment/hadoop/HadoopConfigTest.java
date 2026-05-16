package com.java26groupwork.finalassignment.hadoop;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class HadoopConfigTest {

    @TempDir
    Path tempDir;

    @Test
    void clusterModeLoadsMapredSiteSoJobsUseYarn() throws IOException {
        writeConfig("core-site.xml", """
                <configuration>
                  <property>
                    <name>fs.defaultFS</name>
                    <value>hdfs://orbha</value>
                  </property>
                </configuration>
                """);
        writeConfig("hdfs-site.xml", """
                <configuration>
                  <property>
                    <name>dfs.nameservices</name>
                    <value>orbha</value>
                  </property>
                </configuration>
                """);
        writeConfig("mapred-site.xml", """
                <configuration>
                  <property>
                    <name>mapreduce.framework.name</name>
                    <value>yarn</value>
                  </property>
                </configuration>
                """);
        writeConfig("yarn-site.xml", """
                <configuration>
                  <property>
                    <name>yarn.resourcemanager.cluster-id</name>
                    <value>orb-yarn</value>
                  </property>
                </configuration>
                """);

        HadoopProperties properties = new HadoopProperties();
        properties.setMode(HadoopProperties.Mode.CLUSTER);
        properties.setConfigDir(tempDir.toString());

        org.apache.hadoop.conf.Configuration configuration = new HadoopConfig().hadoopConfiguration(properties);

        assertThat(configuration.get("fs.defaultFS")).isEqualTo("hdfs://orbha");
        assertThat(configuration.get("dfs.nameservices")).isEqualTo("orbha");
        assertThat(configuration.get("mapreduce.framework.name")).isEqualTo("yarn");
        assertThat(configuration.get("yarn.resourcemanager.cluster-id")).isEqualTo("orb-yarn");
        assertThat(configuration.getClass("fs.AbstractFileSystem.hdfs.impl", null)).isNotNull();
    }

    private void writeConfig(String fileName, String content) throws IOException {
        Files.writeString(tempDir.resolve(fileName), content);
    }
}
