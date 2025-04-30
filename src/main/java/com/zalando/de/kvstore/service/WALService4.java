package com.zalando.de.kvstore.service;

import com.zalando.de.kvstore.core.KVEntity;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

public class WALService4 implements WALInterface {

    @Override
    public Future<Void> write(String key, String val) throws IOException {
        write2(key, val);
        return new CompletableFuture<>();
    }

    public static final int RECORD_START = 0xDEADDEAD;
    public static final String WAL_LOG = "wal.log";

    static class WALEntity {

        long logNumber;
        String key;

        public WALEntity(long logNumber, String key, String val) {
            this.logNumber = logNumber;
            this.key = key;
            this.val = val;
        }

        String val;


    }

    ;


    private BlockingQueue<WALEntity> buffer;
    private volatile long logNumber;
    private Lock lock;
    private AtomicBoolean isRunning;
    Thread writer;

    private RandomAccessFile raf;

    public WALService4() throws FileNotFoundException {
        buffer = new LinkedBlockingQueue<>(1000);
        lock = new ReentrantLock();
        isRunning = new AtomicBoolean(true);
        logNumber = 0;
        raf = new RandomAccessFile(WAL_LOG, "rw");
        writer = new Thread() {
            @Override
            public void run() {
                while (isRunning.get()) {
                    lock.lock();
                    try {
                        WALEntity walEntity = buffer.take();
                        writeToFile(List.of(walEntity));
                    } catch (InterruptedException | IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        lock.unlock();
                    }
                }
            }
        };
        writer.start();
    }

    private long crc(byte[] a1, byte[] a2) {
        CRC32 crc32 = new CRC32();
        crc32.update(a1);
        crc32.update(a2);
        return crc32.getValue();
    }
    private void writeToFile(List<WALEntity> records) throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
            for (WALEntity walEntity : records) {
                dataOutputStream.writeInt(RECORD_START);
                dataOutputStream.writeLong(crc(walEntity.key.getBytes(), walEntity.val.getBytes()));
                dataOutputStream.writeUTF(walEntity.key);
                dataOutputStream.writeUTF(walEntity.val);
            }
            byte[] data = byteArrayOutputStream.toByteArray();
            raf.write(data);
            raf.getFD().sync();
        }
    }


    public long write2(String key, String val) throws IOException {
        lock.lock();
        try {

            WALEntity walEntity = new WALEntity(logNumber++, key, val);
            buffer.add(walEntity);
            return walEntity.logNumber;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown() throws IOException {
        isRunning.set(false);
        try {
            writer.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean exists() {
        return false;
    }

    @Override
    public List<KVEntity> recover() throws IOException {
        return null;
    }
    @Override
    public long offset() throws IOException {
        return raf.getFilePointer();
    }
}
