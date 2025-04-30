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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.zip.CRC32;

public class WalService9 implements WALInterface {

    public static final String WAL_LOG = "wal.log";
    public static final int RECORD_BEGIN = 0xCAFEBEBE;

    private static class WALEntry {
        final String key;
        final String val;
        final CompletableFuture<Void> future;

        public WALEntry(String key, String val) {
            this.key = key;
            this.val = val;
            this.future = new CompletableFuture<>();
        }
    }

    private class Worker extends Thread {
        private final BlockingQueue<WALEntry> buffer;
        private final AtomicBoolean isRunning;

        public Worker(String name, BlockingQueue<WALEntry> buffer, AtomicBoolean isRunning) {
            super(name);
            this.buffer = buffer;
            this.isRunning = isRunning;
        }

        @Override
        public void run() {
            while (isRunning.get() || !buffer.isEmpty()) {
                WALEntry walEntry;
                try {
                    walEntry = buffer.poll(100, TimeUnit.MILLISECONDS);
                    if (walEntry != null) {
                        try {
                            write(walEntry);
                            walEntry.future.complete(null);
                        } catch (Exception e) {
                            walEntry.future.completeExceptionally(e);
                        }
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void write(WALEntry walEntry) throws IOException {
        lock.lock();
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {

            dataOutputStream.writeInt(RECORD_BEGIN);
            dataOutputStream.writeLong(crc32(walEntry.key.getBytes(), walEntry.val.getBytes()));
            dataOutputStream.writeUTF(walEntry.key);
            dataOutputStream.writeUTF(walEntry.val);
            randomAccessFile.write(byteArrayOutputStream.toByteArray());
            randomAccessFile.getFD().sync();

        } finally {
            lock.unlock();
        }
    }

    private long crc32(byte[]... bytes) {
        CRC32 crc32 = new CRC32();
        for (byte[] b : bytes) {
            crc32.update(b);
        }
        return crc32.getValue();
    }

    private final RandomAccessFile randomAccessFile;
    private final Lock lock;
    private final Thread worker;
    private final AtomicBoolean isRunning;
    private final BlockingQueue<WALEntry> buffer;

    public WalService9(Lock lock) throws FileNotFoundException {
        this.lock = lock;
        this.buffer = new LinkedBlockingQueue<>();
        this.isRunning = new AtomicBoolean(true);
        this.randomAccessFile = new RandomAccessFile(WAL_LOG, "rw");
        this.worker = new Worker("wal-writer", buffer, isRunning);
        this.worker.start(); /
    }

    @Override
    public Future<Void> write(String key, String val) {
        WALEntry walEntry = new WALEntry(key, val);
        buffer.add(walEntry);
        return walEntry.future;
    }

    @Override
    public void shutdown() throws IOException {
        try {
            isRunning.set(false);
            worker.interrupt();
            worker.join(); // Wait for graceful shutdown
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during WAL shutdown", e);
        } finally {
            randomAccessFile.close();
        }
    }

    @Override
    public boolean exists() {
        return Files.exists(Path.of(WAL_LOG));
    }

    @Override
    public List<KVEntity> recover() throws IOException {
        List<KVEntity> res = new ArrayList<>();
        try (FileInputStream fileInputStream = new FileInputStream(WAL_LOG);
            DataInputStream dataInputStream = new DataInputStream(fileInputStream)) {

            while (true) {
                try {
                    if (dataInputStream.readInt() != RECORD_BEGIN) {
                        continue;
                    }
                    long crc = dataInputStream.readLong();
                    String key = dataInputStream.readUTF();
                    String val = dataInputStream.readUTF();
                    if (crc == crc32(key.getBytes(), val.getBytes())) {
                        res.add(KVEntity.builder().key(key).val(val).build());
                    }
                } catch (EOFException eof) {
                    break; // ✅ Properly exit loop
                }
            }

        }
        return res;
    }
}
