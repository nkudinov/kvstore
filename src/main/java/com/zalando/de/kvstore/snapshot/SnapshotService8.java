package com.zalando.de.kvstore.snapshot;

import com.zalando.de.kvstore.core.KVStore;
import com.zalando.de.kvstore.service.WALInterface;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.locks.Lock;

public class SnapshotService8 {

    public static final String SNAPSHOT_TMP = "snapshot.tmp";
    public static final String SNAPSHOT_LOG = "snapshot.log";
    private Lock lock;
    private WALInterface wal;
    private KVStore kvStore;

    public SnapshotService8(Lock lock, WALInterface wal, KVStore kvStore) {
        this.lock = lock;
        this.wal = wal;
        this.kvStore = kvStore;
    }

    public void take() {
        lock.lock();
        try (DataOutputStream dataOutputStream = new DataOutputStream(
            new BufferedOutputStream(new FileOutputStream(SNAPSHOT_TMP)))) {
            dataOutputStream.writeLong(0L); // offset
            for (Map.Entry<String, String> entry : kvStore.getStore().entrySet()) {
                dataOutputStream.write();
                dataOutputStream.writeUTF(entry.getKey());
                dataOutputStream.writeUTF(entry.getValue());
            }
            Files.move(Path.of(SNAPSHOT_TMP), Path.of(SNAPSHOT_LOG));
        } catch (IOException e) {
            e.printStackTrace(); // Consider logging instead of printing in production
        } finally {
            lock.unlock();
        }
    }
}
