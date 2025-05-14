package com.zalando.de.kvstore.snapshot;

import com.zalando.de.kvstore.core.KVStore;
import com.zalando.de.kvstore.service.WALInterface;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.locks.Lock;

public class SimpleSnapshotService11 {
    public static final String SNAPSHOT_TMP = "snapshot.tmp";
    public static final String SNAPSHOT_LOG = "snapshot.log";

    private final Lock lock;
    private final WALInterface wal;
    private final KVStore kvStore;

    public SimpleSnapshotService11(Lock lock, WALInterface wal, KVStore kvStore) {
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
        Files.move(
            Path.of(SNAPSHOT_TMP),
            Path.of(SNAPSHOT_LOG),
            StandardCopyOption.ATOMIC_MOVE
        );
    }

    private void createSnapshot() throws IOException {
        try (DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(SNAPSHOT_TMP))){
            // Write WAL offset
            dataOutputStream.writeLong(wal.offset());
            for(Map.Entry<String,String> entry:kvStore.getStore().entrySet()) {
                  dataOutputStream.writeInt(0xCAFECAFE);
                  dataOutputStream.writeUTF(entry.getKey());
                  dataOutputStream.writeUTF(entry.getValue());
            }
            dataOutputStream.flush();
        }
    }
}
