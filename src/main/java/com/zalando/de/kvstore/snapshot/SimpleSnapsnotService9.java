package com.zalando.de.kvstore.snapshot;

import com.zalando.de.kvstore.core.KVStore;
import com.zalando.de.kvstore.service.WALInterface;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.locks.Lock;

public class SimpleSnapsnotService9 {

    public static final String SNAPSHOT_TMP = "snapshot.tmp";
    public static final String SNAPSHOT_LOG = "snapshot.log";

    private final Lock lock;
    private final WALInterface wal;
    private final KVStore kvStore;

    public SimpleSnapshotService9(Lock lock, WALInterface wal, KVStore kvStore) {
        this.lock = lock;
        this.wal = wal;
        this.kvStore = kvStore;
    }

    public void takeSnapshot() throws IOException {
        lock.lock();
        try {
            createSnapshot();
            renameAtomically();
        } finally {
            lock.unlock();
        }
    }

    private void renameAtomically() throws IOException {
        Files.move(Path.of(SNAPSHOT_TMP), Path.of(SNAPSHOT_LOG),
            StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private void createSnapshot() throws IOException {
        try (DataOutputStream out = new DataOutputStream(
            new BufferedOutputStream(new FileOutputStream(SNAPSHOT_TMP)))) {

            // Write a placeholder WAL offset — this should be actual offset in a full implementation
            out.writeLong(wal.offset()); // TODO: fetch actual WAL offset

            for (Map.Entry<String, String> entry : kvStore.getStore().entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
            out.flush();
        }
    }
}
