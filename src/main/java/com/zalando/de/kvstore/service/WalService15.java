package com.zalando.de.kvstore.service;

import com.zalando.de.kvstore.core.KVEntity;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.zip.CRC32;

public class WalService15 implements WALInterface {

    private static final String WAL_LOG = "wal.log";
    private static final int MAGIC_HEADER = 0xCAFEBABE;

    private final FileChannel fileChannel;
    private final Lock lock;
    private final BlockingQueue<WALEntry> queue = new LinkedBlockingQueue<>();
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final Thread worker;

    private static class WALEntry {
        final String key;
        final String value;
        final CompletableFuture<Void> future = new CompletableFuture<>();

        WALEntry(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    public WalService15(Lock lock) {
        this.lock = lock;
        try {
            this.fileChannel = FileChannel.open(
                Paths.get(WAL_LOG),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND,
                StandardOpenOption.SYNC
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to open WAL log file", e);
        }

        this.worker = new Thread(this::processEntries, "wal-worker-thread");
        this.worker.start();
    }

    private void processEntries() {
        while (isRunning.get() || !queue.isEmpty()) {
            try {
                WALEntry entry = queue.poll(100, TimeUnit.MILLISECONDS);
                if (entry != null) {
                    writeToFile(entry.key, entry.value);
                    entry.future.complete(null);
                }
            } catch (Exception e) {
                e.printStackTrace(); // Log error
            }
        }
    }

    private void writeToFile(String key, String value) throws IOException {
        lock.lock();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos)) {

            dos.writeInt(MAGIC_HEADER);
            dos.writeUTF(key);
            dos.writeUTF(value);
            dos.writeLong(calculateCRC32(key, value));

            fileChannel.write(ByteBuffer.wrap(baos.toByteArray()));
            fileChannel.force(true);
        } finally {
            lock.unlock();
        }
    }

    private long calculateCRC32(String key, String value) {
        CRC32 crc = new CRC32();
        crc.update(key.getBytes());
        crc.update(value.getBytes());
        return crc.getValue();
    }

    @Override
    public boolean exists() {
        return Files.exists(Path.of(WAL_LOG));
    }

    @Override
    public Future<Void> write(String key, String val) {
        WALEntry entry = new WALEntry(key, val);
        try {
            queue.put(entry);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("WAL write interrupted", e);
        }
        return entry.future;
    }

    @Override
    public void shutdown() throws IOException {
        isRunning.set(false);
        worker.interrupt();
        try {
            worker.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        fileChannel.close();
    }

    @Override
    public List<KVEntity> recover() throws IOException {
        List<KVEntity> entries = new ArrayList<>();
        Path path = Path.of(WAL_LOG);

        if (!Files.exists(path)) return entries;

        try (DataInputStream dis = new DataInputStream(Files.newInputStream(path))) {
            while (true) {
                try {
                    int magic = dis.readInt();
                    if (magic != MAGIC_HEADER) break;

                    String key = dis.readUTF();
                    String value = dis.readUTF();
                    long checksum = dis.readLong();

                    if (checksum == calculateCRC32(key, value)) {
                        entries.add(KVEntity.builder().key(key).val(value).build());
                    }
                } catch (EOFException e) {
                    break; // Normal termination
                }
            }
        }

        return entries;
    }

    @Override
    public long offset() throws IOException {
        lock.lock();
        try {
            return fileChannel.position();
        } finally {
            lock.unlock();
        }
    }
}
