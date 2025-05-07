package com.zalando.de.kvstore.snapshot;

import com.zalando.de.kvstore.core.KVEntity;
import com.zalando.de.kvstore.core.KVStore;
import com.zalando.de.kvstore.service.WALInterface;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.locks.Lock;

public class SimpleSnapsnotService10 {
    private final Path snapshotTmpPath;
    private final Path snapshotFinalPath;
    private final Lock lock;
    private final KVStore kvStore;
    private final WALInterface walInterface;

    public SimpleSnapsnotService10(
        Lock lock,
        KVStore kvStore,
        WALInterface walInterface,
        String snapshotTmpFilename,
        String snapshotFinalFilename
    ) {
        this.lock = lock;
        this.kvStore = kvStore;
        this.walInterface = walInterface;
        this.snapshotTmpPath = Path.of(snapshotTmpFilename);
        this.snapshotFinalPath = Path.of(snapshotFinalFilename);
    }

    public void takeSnapshot() throws IOException {
        lock.lock();
        try (DataOutputStream out = new DataOutputStream(
            new BufferedOutputStream(new FileOutputStream(snapshotTmpPath.toFile())))) {

            // Write WAL offset
            out.writeLong(walInterface.offset());

            // Write all key-value entries
            for (Map.Entry<String, String> entry : kvStore.getStore().entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }

            out.flush(); // Ensure everything is written
        } finally {
            lock.unlock();
        }

        // Move snapshot.tmp to snapshot.log atomically
        Files.move(snapshotTmpPath, snapshotFinalPath,
            StandardCopyOption.REPLACE_EXISTING,
            StandardCopyOption.ATOMIC_MOVE);
    }
}
