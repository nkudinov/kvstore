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
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SnapshotService8 {
    private static final Logger logger = Logger.getLogger(SnapshotService8.class.getName());

    private static final String SNAPSHOT_TMP = "snapshot.tmp";
    private static final String SNAPSHOT_FILE = "snapshot.log";
    private static final byte ENTRY_MARKER = 1;

    private final Lock lock;
    private final WALInterface wal;
    private final KVStore kvStore;

    public SnapshotService8(Lock lock, WALInterface wal, KVStore kvStore) {
        this.lock = lock;
        this.wal = wal;
        this.kvStore = kvStore;
    }

    public void takeSnapshot() {
        lock.lock();
        try {
            writeSnapshotToTempFile();
            replaceOldSnapshot();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to take snapshot", e);
        } finally {
            lock.unlock();
        }
    }

    private void writeSnapshotToTempFile() throws IOException {
        try (DataOutputStream out = new DataOutputStream(
            new BufferedOutputStream(new FileOutputStream(SNAPSHOT_TMP)))) {

            out.writeLong(0L); // offset placeholder

            for (Map.Entry<String, String> entry : kvStore.getStore().entrySet()) {
                out.writeByte(ENTRY_MARKER); // marks beginning of entry
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
        }
    }

    private void replaceOldSnapshot() throws IOException {
        Path tmpPath = Path.of(SNAPSHOT_TMP);
        Path snapshotPath = Path.of(SNAPSHOT_FILE);

        // Atomic move to avoid corrupted snapshot file
        Files.move(tmpPath, snapshotPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
