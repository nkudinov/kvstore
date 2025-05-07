package com.zalando.de.kvstore.service;

import com.zalando.de.kvstore.core.KVEntity;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
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

public class WalService10 implements WALInterface {

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

    private BlockingQueue<WALEntry> buffer;
    private Lock lock;
    private final RandomAccessFile randomAccessFile;
    private final String walFileName;

    private final AtomicBoolean isRunning;

    private class Worker extends Thread {

        @Override
        public void run() {
            WALEntry walEntry = null;
            while (isRunning.get()) {
                try {
                    walEntry = buffer.poll(1000, TimeUnit.MICROSECONDS);
                    if (walEntry != null) {
                        writeEntry(walEntry.key, walEntry.val);
                        walEntry.future.complete(null);
                    }
                } catch (InterruptedException | IOException e) {
                    if (walEntry != null) {
                        walEntry.future.completeExceptionally(e);
                    }
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private final Worker worker;

    private void writeEntry(String key, String val) throws IOException {
        lock.lock();
        try {
            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
                dataOutputStream.writeInt(0xBEBEBEBE);
                dataOutputStream.writeLong(crc32(key.getBytes(), val.getBytes()));
                dataOutputStream.writeUTF(key);
                dataOutputStream.writeUTF(val);
                randomAccessFile.write(byteArrayOutputStream.toByteArray());
            }
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

    public WalService10(Lock lock, String walFileName) throws FileNotFoundException {
        this.lock = lock;
        this.walFileName = walFileName;
        this.randomAccessFile = new RandomAccessFile(walFileName, "rw");
        this.buffer = new LinkedBlockingQueue<>();
        this.isRunning = new AtomicBoolean(true);
        worker = new Worker();
        worker.start();
    }

    @Override
    public Future<Void> write(String key, String val) throws IOException {
        WALEntry walEntry = new WALEntry(key, val);
        buffer.add(walEntry);
        return walEntry.future;
    }

    @Override
    public void shutdown() throws IOException {
        isRunning.set(false);
        try {
            worker.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        randomAccessFile.close();
    }

    @Override
    public boolean exists() {
        return Files.exists(Path.of(walFileName));
    }

    @Override
    public List<KVEntity> recover() throws IOException {
        return new ArrayList<>();
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
