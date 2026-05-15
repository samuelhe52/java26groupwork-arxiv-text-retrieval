package com.java26groupwork.finalassignment.hadoop;

import java.io.IOException;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Windows local-dev fallback that avoids winutils-only permission calls while still
 * exercising Hadoop's local MapReduce execution path.
 */
public final class LocalDevFileSystem extends LocalFileSystem {

    public LocalDevFileSystem() {
        super(new NoOpPermissionRawLocalFileSystem());
    }

    private static final class NoOpPermissionRawLocalFileSystem extends RawLocalFileSystem {
        @Override
        public void setPermission(Path path, FsPermission permission) throws IOException {
            // RawLocalFileSystem delegates to winutils on Windows. Local dev mode only needs
            // readable/writable temp files, so skip the permission mutation here.
        }
    }
}
