package com.java26groupwork.finalassignment.corpus;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.demo")
public class PrebuiltDemoProperties {

    private String prebuiltSnapshotPath = "../datasets/prebuilt-cluster-demo/snapshot.json";

    public String getPrebuiltSnapshotPath() {
        return prebuiltSnapshotPath;
    }

    public void setPrebuiltSnapshotPath(String prebuiltSnapshotPath) {
        this.prebuiltSnapshotPath = prebuiltSnapshotPath;
    }
}
