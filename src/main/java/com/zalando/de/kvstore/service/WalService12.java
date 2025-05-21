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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.zip.CRC32;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WalService12 implements WALInterface {

    private static final int CAPACITY = 1000;
    private static final int MAGIC_NUMBER = 0xCAFEBEBE;

    private final Path walPath;
    private final RandomAccessFile randomAccessFile;
    private final BlockingQueue<WALEntry> buffer;
    private final AtomicBoolean isRunning;
    private final Lock lock;
    private final Thread workerThread;

    private class WALEntry {
        final String key;
        final String val;
        final CompletableFuture<Void> future = new CompletableFuture<>();

        WALEntry(String key, String val) {
            this.key = key;
            this.val = val;
        }
    }

    private class Worker extends Thread {
        @Override
        public void run() {
            while (isRunning.get() || !buffer.isEmpty()) {
                WALEntry walEntry = null;
                try {
                    walEntry = buffer.poll(1, TimeUnit.SECONDS);
                    if (walEntry != null) {
                        writeSingleRecord(walEntry);
                        walEntry.future.complete(null);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Preserve interrupt status
                    if (walEntry != null) {
                        walEntry.future.completeExceptionally(e);
                    }
                    break;
                } catch (Exception e) {
                    if (walEntry != null) {
                        walEntry.future.completeExceptionally(e);
                    }
                    log.error("Error writing WAL entry", e);
                }
            }
            log.info("WAL worker stopped.");
        }
    }

    public WalService12(Lock lock) throws IOException {
        this(Path.of("wal.log"), lock);
    }

    public WalService12(Path walPath, Lock lock) throws IOException {
        this.lock = lock;
        this.walPath = walPath;
        this.randomAccessFile = new RandomAccessFile(walPath.toFile(), "rw");
        this.buffer = new ArrayBlockingQueue<>(CAPACITY);
        this.isRunning = new AtomicBoolean(true);
        this.workerThread = new Worker();
        this.workerThread.start();
    }

    @Override
    public Future<Void> write(String key, String val) throws IOException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty.");
        }

        WALEntry entry = new WALEntry(key, val);
        try {
            if (!buffer.offer(entry, 1, TimeUnit.SECONDS)) {
                throw new IOException("WAL buffer is full.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while offering to WAL buffer.", e);
        }
        return entry.future;
    }

    private void writeSingleRecord(WALEntry entry) throws IOException {
        lock.lock();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos)) {

            dos.writeInt(MAGIC_NUMBER);
            dos.writeLong(computeCRC32(entry.key, entry.val));
            dos.writeUTF(entry.key);
            dos.writeUTF(entry.val);

            byte[] record = baos.toByteArray();
            randomAccessFile.write(record);
            randomAccessFile.getFD().sync();
        } finally {
            lock.unlock();
        }
    }

    private long computeCRC32(String... inputs) {
        CRC32 crc = new CRC32();
        for (String input : inputs) {
            crc.update(input.getBytes());
        }
        return crc.getValue();
    }

    @Override
    public void shutdown() throws IOException {
        isRunning.set(false);
        workerThread.interrupt();
        try {
            workerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while shutting down WAL", e);
        }
        randomAccessFile.close();
    }

    @Override
    public boolean exists() {
        return Files.exists(walPath);
    }

    @Override
    public List<KVEntity> recover() throws IOException {
        List<KVEntity> recovered = new ArrayList<>();
        lock.lock();
        try (DataInputStream dis = new DataInputStream(new FileInputStream(walPath.toFile()))) {
            while (true) {
                try {
                    int magic = dis.readInt();
                    if (magic != MAGIC_NUMBER) {
                        log.warn("Invalid magic number found. Skipping corrupted record.");
                        continue;
                    }

                    long crc = dis.readLong();
                    String key = dis.readUTF();
                    String val = dis.readUTF();

                    if (crc != computeCRC32(key, val)) {
                        log.warn("CRC mismatch for key={}, skipping record.", key);
                        continue;
                    }

                    recovered.add(KVEntity.builder().key(key).val(val).build());
                } catch (EOFException eof) {
                    log.debug("End of WAL file reached.");
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
        return recovered;
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
