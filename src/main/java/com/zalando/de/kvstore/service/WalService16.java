package com.zalando.de.kvstore.service;

import com.zalando.de.kvstore.core.KVEntity;
import com.zalando.de.kvstore.core.KVStore;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class WalService16 implements WALInterface {

    private AtomicBoolean isRunning;
    private final Worker worker;

    public WalService16() throws IOException {
        fileChannel = FileChannel.open(Path.of("wal.log"), StandardOpenOption.APPEND, StandardOpenOption.SYNC);
        worker = new Worker();
        worker.start();
    }

    private class WALEntry {

        private String key;
        private String value;
        private CompletableFuture<Void> future;

        public WALEntry(String key, String value) {
            this.key = key;
            this.value = value;
            this.future = new CompletableFuture<>();
        }
    }

    private final FileChannel fileChannel;
    private Lock lock = new ReentrantLock();

    private class Worker extends Thread {

        @Override
        public void run() {
            WALEntry walEntry = null;
            while (isRunning.get() && !isInterrupted()) {
                try {
                    walEntry = buffer.poll(100, TimeUnit.MICROSECONDS);
                    if (walEntry != null) {
                        persist(walEntry.key, walEntry.value);
                        walEntry.future.complete(null);
                    }
                } catch (InterruptedException e) {
                    if (walEntry != null) {
                        walEntry.future.completeExceptionally(e);
                    }
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    if (walEntry != null) {
                        walEntry.future.completeExceptionally(e);
                    }
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void persist(String key, String value) throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
            dataOutputStream.writeInt(0xCAFECAFE);
            dataOutputStream.writeUTF(key);
            dataOutputStream.writeUTF(value);
            dataOutputStream.flush();

            fileChannel.write(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
        }
    }

    private final BlockingQueue<WALEntry> buffer = new ArrayBlockingQueue<>(1024);

    @Override
    public boolean exists() {
        return false;
    }

    @Override
    public Future<Void> write(String key, String val) throws IOException {
        WALEntry entry = new WALEntry(key, val);
        try {
            buffer.put(entry);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
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
        return List.of();
    }

    @Override
    public long offset() throws IOException {
        try {
            lock.lock();
            return fileChannel.position();
        } finally {
            lock.unlock();
        }
    }
}
