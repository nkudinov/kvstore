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
import lombok.SneakyThrows;

public class WalService9 implements WALInterface {

    public static final String WAL_LOG = "wal.log";
    public static final int RECORD_BEGIN = 0xCAFEBEBE;

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

        private BlockingQueue<WALEntry> buffer;
        private AtomicBoolean isRunning;

        public Worker(String name, BlockingQueue<WALEntry> buffer, AtomicBoolean isRunning) {
            super(name);
            this.buffer = buffer;
            this.isRunning = isRunning;
        }

        @SneakyThrows
        @Override
        public void run() {
            while (isRunning.get()) {
                WALEntry walEntry = null;
                try {
                    walEntry = buffer.poll(1000, TimeUnit.MICROSECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (walEntry != null) {
                    write(walEntry);
                    walEntry.future.complete(null);
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

    public WalService9(Lock lock) throws FileNotFoundException {
        this.lock = lock;
        this.buffer = new LinkedBlockingQueue<>();
        this.isRunning = new AtomicBoolean(true);
        this.randomAccessFile = new RandomAccessFile(WAL_LOG, "rw");
        this.worker = new Worker("wal-writer", buffer, isRunning);
    }

    private final AtomicBoolean isRunning;
    private final BlockingQueue<WALEntry> buffer;

    @Override
    public Future<Void> write(String key, String val) throws IOException {
        WALEntry walEntry = new WALEntry(key, val);
        buffer.add(walEntry);
        return walEntry.future;
    }

    @Override
    public void shutdown() throws IOException {
        try {
            isRunning.set(false);
            worker.interrupt();
            worker.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
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
                    if (crc != crc32(key.getBytes(), val.getBytes())) {
                        continue;
                    }
                    res.add(KVEntity.builder().key(key).val(val).build());
                } catch (EOFException ignored) {

                }
            }

        }
    }
}
