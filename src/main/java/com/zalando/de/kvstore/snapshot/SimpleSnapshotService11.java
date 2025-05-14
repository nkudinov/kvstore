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
    public static final int MAGIC = 0xCAFECAFE;

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
            log.info("Starting snapshot creation...");
            createSnapshot();
            renameAtomically();
            log.info("Snapshot created successfully.");
        } finally {
            lock.unlock();
        }
    }

    private void createSnapshot() throws IOException {
        Path tmpPath = Path.of(SNAPSHOT_TMP);
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(tmpPath.toFile()))) {
            // Write WAL offset
            long offset = wal.offset();
            dos.writeLong(offset);
            log.debug("Wrote WAL offset: {}", offset);

            int count = 0;
            for (Map.Entry<String, String> entry : kvStore.getStore().entrySet()) {
                dos.writeInt(MAGIC);
                dos.writeUTF(entry.getKey());
                dos.writeUTF(entry.getValue());
                count++;
            }

            dos.flush();
            log.info("Snapshot written with {} entries", count);
        }
    }

    private void renameAtomically() throws IOException {
        Files.move(
            Path.of(SNAPSHOT_TMP),
            Path.of(SNAPSHOT_LOG),
            StandardCopyOption.ATOMIC_MOVE
        );
    }
}
