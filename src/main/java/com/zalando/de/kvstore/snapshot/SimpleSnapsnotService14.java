package com.zalando.de.kvstore.snapshot;

import com.zalando.de.kvstore.core.KVStore;
import com.zalando.de.kvstore.service.WALInterface;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.locks.Lock;

public class SimpleSnapsnotService14 {
    public static final String SNAPSHOT_TMP = "snapshot.tmp";
    public static final String SNAPSHOT_LOG = "snapshot.log";
    public static final int MAGIC = 0xCAFECAFE;

    private final Lock lock;
    private final WALInterface wal;
    private final KVStore kvStore;
    public SimpleSnapsnotService14(Lock lock, WALInterface wal, KVStore kvStore) {
        this.lock = lock;
        this.wal = wal;
        this.kvStore = kvStore;
    }
    public void takeSnapshot() throws IOException {
        lock.lock();
        try {
            write(kvStore);
            move(SNAPSHOT_TMP, SNAPSHOT_LOG);
        } finally {
            lock.unlock();
        }
    }
    private void write(KVStore kvStore) {
        try (FileChannel fileChannel = FileChannel.open(
            Path.of(SNAPSHOT_TMP),
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE, StandardOpenOption.DSYNC)) {

            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {

                for (Map.Entry<String, String> entry : kvStore.getStore().entrySet()) {
                    dataOutputStream.writeInt(MAGIC); // use consistent magic
                    dataOutputStream.writeUTF(entry.getKey());
                    dataOutputStream.writeUTF(entry.getValue());
                }

                dataOutputStream.flush();
                fileChannel.write(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
                fileChannel.force(true);
            }

        } catch (IOException e) {
            throw new RuntimeException("Snapshot write failed", e);
        }
    }
    private void move(String snapshotTmp, String snapshotLog) throws IOException {
        Files.move(Path.of(snapshotTmp), Path.of(snapshotLog), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
