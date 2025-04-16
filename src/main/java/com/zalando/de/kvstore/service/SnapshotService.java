package com.zalando.de.kvstore.service;

import com.zalando.de.kvstore.core.KVStore;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;


@Slf4j
public class SnapshotService {

    private static final String SNAPSHOT_FILE = "snapshot.dat";
    private static final String TEMP_SNAPSHOT_FILE = "snapshot.tmp";
    private static final int MAGIC_HEADER = 0xCAFECAFE;

    private final KVStore kvStore;
    private final Lock lock;
    private WALInterface walInterface;

    public SnapshotService(KVStore kvStore, Lock lock, WALInterface walInterface) {
        this.kvStore = kvStore;
        this.lock = lock;
    }

    public void takeSnapshot() throws IOException {
        lock.lock();
        try {
            Path tempPath = Path.of(TEMP_SNAPSHOT_FILE);
            try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(tempPath))) {
                long snapshotVersion = System.currentTimeMillis(); // or WAL offset
                out.writeLong(snapshotVersion);

                Map<String, String> store = kvStore.getStore();
                for (Map.Entry<String, String> entry : store.entrySet()) {
                    out.writeInt(MAGIC_HEADER);
                    out.writeUTF(entry.getKey());
                    out.writeUTF(entry.getValue());
                }

                out.flush();
                log.info("Snapshot written to temp file ({} entries)", store.size());
            }

            Files.move(tempPath, Path.of(SNAPSHOT_FILE), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            log.info("Snapshot saved successfully to {}", SNAPSHOT_FILE);
        } finally {
            lock.unlock();
        }
    }
}
