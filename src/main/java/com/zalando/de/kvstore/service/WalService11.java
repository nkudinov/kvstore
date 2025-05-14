package com.zalando.de.kvstore.service;

import com.zalando.de.kvstore.core.KVEntity;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WalService11 implements WALInterface {
    public static final int CAPACITY = 1000;
    public static final String WAL_FILE_NAME = "wal.log";

    private final BlockingQueue<WALEntry> buffer;
    private final Lock lock;
    private final AtomicBoolean isRunning;
    private final RandomAccessFile randomAccessFile;
    private final Worker worker;

    private class WALEntry {
        private final String key;
        private final String val;
        private final CompletableFuture<Void> future;

        public WALEntry(String key, String val) {
            this.key = key;
            this.val = val;
            this.future = new CompletableFuture<>();
        }
    }

    private class Worker extends Thread {
        private final BlockingQueue<WALEntry> queue;
        private final AtomicBoolean isRunning;

        private Worker(BlockingQueue<WALEntry> queue, AtomicBoolean isRunning) {
            super("WAL-Worker");
            this.queue = queue;
            this.isRunning = isRunning;
        }

        @Override
        public void run() {
            while (isRunning.get() || !queue.isEmpty()) {
                try {
                    WALEntry walEntry = queue.poll(1000, TimeUnit.MILLISECONDS);
                    if (walEntry != null) {
                        write(walEntry);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Worker thread interrupted", e);
                } catch (IOException e) {
                    log.error("Error writing WAL entry", e);
                }
            }
        }
    }

    public WalService11(Lock lock) throws FileNotFoundException {
        if (lock == null) {
            throw new IllegalArgumentException("lock must not be null");
        }
        this.lock = lock;
        this.buffer = new LinkedBlockingQueue<>(CAPACITY);
        this.isRunning = new AtomicBoolean(true);
        this.randomAccessFile = new RandomAccessFile(WAL_FILE_NAME, "rw");
        this.worker = new Worker(buffer, isRunning);
        this.worker.start();
    }

    @Override
    public Future<Void> write(String key, String val) {
        WALEntry walEntry = new WALEntry(key, val);
        buffer.add(walEntry);
        return walEntry.future;
    }

    private void write(WALEntry walEntry) throws IOException {
        lock.lock();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos)) {

            dos.writeInt(0xCAFEBEBE);
            dos.writeLong(crc32(walEntry.key, walEntry.val));
            dos.writeUTF(walEntry.key);
            dos.writeUTF(walEntry.val);

            randomAccessFile.write(baos.toByteArray());
            randomAccessFile.getFD().sync();

            walEntry.future.complete(null);
        } catch (IOException e) {
            walEntry.future.completeExceptionally(e);
            throw e;
        } finally {
            lock.unlock();
        }
    }

    private long crc32(String... strings) {
        CRC32 crc32 = new CRC32();
        for (String str : strings) {
            crc32.update(str.getBytes(StandardCharsets.UTF_8));
        }
        return crc32.getValue();
    }

    @Override
    public void shutdown() throws IOException {
        isRunning.set(false);
        try {
            worker.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Shutdown interrupted while waiting for worker thread", e);
        }
        randomAccessFile.close();
        log.info("WAL service shut down successfully");
    }

    @Override
    public boolean exists() {
        return Files.exists(Path.of(WAL_FILE_NAME));
    }

    @Override
    public List<KVEntity> recover() throws IOException {
        List<KVEntity> result = new ArrayList<>();
        try (DataInputStream dis = new DataInputStream(new FileInputStream(WAL_FILE_NAME))) {
            while (true) {
                try {
                    if (dis.readInt() != 0xCAFEBEBE) {
                        log.warn("Invalid magic number, skipping...");
                        continue;
                    }

                    long crc = dis.readLong();
                    String key = dis.readUTF();
                    String val = dis.readUTF();

                    if (crc != crc32(key, val)) {
                        log.warn("Corrupted record for key '{}', skipping...", key);
                        continue;
                    }

                    result.add(KVEntity.builder().key(key).val(val).build());
                } catch (EOFException e) {
                    break; // End of file reached
                }
            }
        }
        return result;
    }

    @Override
    public long offset() throws IOException {
        lock.lock();
        try {
            return randomAccessFile.getFilePointer();
        } finally {
            lock.unlock();
        }
    }
}
