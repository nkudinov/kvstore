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

    public static final String WAL_LOG = "wal.log";

    private final FileChannel fileChannel;

    private final Lock lock;

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

    private final BlockingQueue<WALEntry> queue;

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private class Worker extends Thread {

        @Override
        public void run() {
            WALEntry walEntry = null;
            while (isRunning.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    walEntry = queue.poll(100, TimeUnit.MICROSECONDS);
                    writeIntoFile(walEntry.key, walEntry.value);
                    if (walEntry != null) {
                        walEntry.future.complete(null);
                    }
                    walEntry = null;
                } catch (Exception e) {
                    if (walEntry != null) {
                        walEntry.future.completeExceptionally(e);
                    }
                }
            }
        }
    }

    private void writeIntoFile(String key, String value) throws IOException {
        lock.lock();
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {

            dataOutputStream.writeInt(0xCAFEBABE);
            dataOutputStream.writeUTF(key);
            dataOutputStream.writeUTF(value);
            dataOutputStream.writeLong(crc32(key, value));

            fileChannel.write(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
            fileChannel.force(true);
        } finally {
            lock.unlock();
        }
    }

    private long crc32(String key, String value) {
        CRC32 crc32 = new CRC32();
        crc32.update(ByteBuffer.wrap(key.getBytes()));
        crc32.update(ByteBuffer.wrap(value.getBytes()));
        return crc32.getValue();
    }

    public WalService15(Lock lock) {
        this.lock = lock;
        try {
            fileChannel = FileChannel.open(Paths.get(WAL_LOG), StandardOpenOption.WRITE, StandardOpenOption.SYNC);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        queue = new LinkedBlockingQueue<>();
    }

    @Override
    public boolean exists() {
        return Files.exists(Path.of(WAL_LOG));
    }

    @Override
    public Future<Void> write(String key, String val) throws IOException {
        WALEntry walEntry = new WALEntry(key, val);
        try {
            queue.put(walEntry);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return walEntry.future;
    }

    @Override
    public void shutdown() throws IOException {
        isRunning.set(false);
    }

    @Override
    public List<KVEntity> recover() throws IOException {
        List<KVEntity> kvEntities = new ArrayList<>();
        try (DataInputStream dataInputStream = new DataInputStream(Files.newInputStream(Path.of(WAL_LOG)))) {
            while (true) {
                if (dataInputStream.readInt() == 0xCAFEBABE) {
                    System.out.println("skip for now");
                }
                String key = dataInputStream.readUTF();
                String value = dataInputStream.readUTF();
                long crc32 = dataInputStream.readLong();
                if (crc32 == crc32(key, value)) {
                    kvEntities.add(KVEntity.builder().key(key).val(value).build());
                }
            }
        } catch (EOFException e) {

        }
        return kvEntities;
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
