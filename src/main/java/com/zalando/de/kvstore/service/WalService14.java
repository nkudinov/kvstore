package com.zalando.de.kvstore.service;

import com.zalando.de.kvstore.core.KVEntity;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
public class WalService14 implements WALInterface {

    public static final int CAPACITY = 1000;
    public static final String WAL_LOG = "wal.log";
    public static final int MAGIC_NUMBER = 0xCAFEBEBE;

    private class WalEntry {

        private final String key;
        private final String val;

        private final CompletableFuture<Void> future;

        public WalEntry(String key, String val) {
            this.key = key;
            this.val = val;
            this.future = new CompletableFuture<>();
        }
    }

    private final AtomicBoolean isRunning;

    private final BlockingQueue<WalEntry> buffer;

    private class Worker extends Thread {

        @Override
        public void run() {
            while ((isRunning.get() || !buffer.isEmpty()) && !Thread.interrupted()) {
                WalEntry walEntry = null;
                try {
                    walEntry = buffer.poll(1000, TimeUnit.MILLISECONDS);
                    write(walEntry);
                    if (walEntry != null) {
                        walEntry.future.complete(null);
                    }
                } catch (InterruptedException | IOException e) {
                    if (walEntry != null) {
                        walEntry.future.completeExceptionally(e);
                    }
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void write(WalEntry walEntry) throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {

            dataOutputStream.writeInt(MAGIC_NUMBER);
            dataOutputStream.writeUTF(walEntry.key);
            dataOutputStream.writeUTF(walEntry.val);
            dataOutputStream.writeLong(crc32(walEntry.key, walEntry.val));

            fileChannel.write(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
            fileChannel.force(true);
        }
    }

    private long crc32(String... vals) {
        CRC32 crc32 = new CRC32();
        for (String v : vals) {
            crc32.update(v.getBytes());
        }
        return crc32.getValue();
    }

    private Worker worker;
    private final Lock lock;

    FileChannel fileChannel;

    public WalService14(Lock lock) throws IOException {
        this.lock = lock;
        buffer = new ArrayBlockingQueue<>(CAPACITY);
        isRunning = new AtomicBoolean(true);
        worker = new Worker();
        worker.start();
        fileChannel = FileChannel.open(Path.of(WAL_LOG), StandardOpenOption.APPEND, StandardOpenOption.DSYNC);
    }

    @Override
    public Future<Void> write(String key, String val) throws IOException {
        WalEntry walEntry = new WalEntry(key, val);
        try {
            buffer.offer(walEntry, 100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return walEntry.future;
    }

    @Override
    public void shutdown() throws IOException {
        isRunning.set(false);
        worker.interrupt();
        try {
            worker.join();
            fileChannel.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
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
            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
            DataInputStream dataInputStream = new DataInputStream(bufferedInputStream);
        ) {
            while (true) {
                if (dataInputStream.readInt() != MAGIC_NUMBER) {
                    log.info("skip till next correct record");
                }
                String key = dataInputStream.readUTF();
                String val = dataInputStream.readUTF();
                long crc = dataInputStream.readLong();
                if (crc == crc32(key, val)) {
                    res.add(KVEntity.builder().key(key).val(val).build());
                }
            }
        } catch (EOFException e) {
            log.info("Reached EOF during WAL recovery.");
        }
        return res;
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
