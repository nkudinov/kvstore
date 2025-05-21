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

public class SnapshotService9 {


    public static final int MAGIC_NUMBER = 0xCAFEBEBE;
    public static final String SNAPSHOT_TMP = "snapshot.tmp";
    public static final String SNAPSHOT_LOG = "snapshot.log";
    private Lock lock;
    private WALInterface walInterface;
    private KVStore kvStore;

    public SnapshotService9(Lock lock, WALInterface walInterface, KVStore kvStore) {
        this.lock = lock;
        this.walInterface = walInterface;
        this.kvStore = kvStore;
    }

    public void takeSnapshot() throws IOException {
        lock.lock();
        try {
            try (DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(SNAPSHOT_TMP))) {
                dataOutputStream.writeLong(walInterface.offset());
                for (Map.Entry<String, String> entry : kvStore.getStore().entrySet()) {
                    dataOutputStream.writeLong(MAGIC_NUMBER);
                    dataOutputStream.writeUTF(entry.getKey());
                    dataOutputStream.writeUTF(entry.getValue());
                }
                dataOutputStream.flush();
            }
        } finally {
            lock.unlock();
        }
        Files.move(Path.of(SNAPSHOT_TMP), Path.of(SNAPSHOT_LOG), StandardCopyOption.REPLACE_EXISTING,
            StandardCopyOption.REPLACE_EXISTING);
    }

}
