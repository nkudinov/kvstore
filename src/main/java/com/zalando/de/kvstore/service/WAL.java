package com.zalando.de.kvstore.service;

import com.zalando.de.kvstore.core.KVEntity;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

public class WAL {

    private static final int MAGIC_NUMBER = 0xCAFEBABE;
    private static final int BATCH_SIZE = 10;
    private static final long POLL_TIMEOUT = 10; // Microseconds

    private final BlockingQueue<KVEntity> buffer;
    private final Lock lock;
    private final RandomAccessFile raf;
    private final Thread worker;
    private volatile boolean running = true; // Graceful shutdown flag

    public WAL() throws IOException {
        this.buffer = new LinkedBlockingQueue<>();
        this.lock = new ReentrantLock();
        this.raf = new RandomAccessFile("wal.log", "rw");

        worker = new Thread(() -> {
            while (running) {
                List<KVEntity> list = new ArrayList<>();
                try {
                    // Poll first entity
                    KVEntity entity = buffer.poll(POLL_TIMEOUT, TimeUnit.MICROSECONDS);
                    if (entity != null) {
                        list.add(entity);
                        // Try draining up to BATCH_SIZE
                        buffer.drainTo(list, BATCH_SIZE - 1);
                    }
                    // Write only if we have data
                    if (!list.isEmpty()) {
                        writeBatch(list);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Restore interrupt flag
                } catch (IOException e) {
                    e.printStackTrace(); // Log error instead of crashing
                }
            }
        });
        worker.start();
    }

    private void writeBatch(List<KVEntity> list) throws IOException {
        lock.lock();
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {

            for (KVEntity kvEntity : list) {
                byte[] keyBytes = kvEntity.getKey().getBytes(StandardCharsets.UTF_8);
                byte[] valBytes = kvEntity.getVal().getBytes(StandardCharsets.UTF_8);
                long checksum = calculateChecksum(keyBytes, valBytes);

                dataOutputStream.writeInt(MAGIC_NUMBER);
                dataOutputStream.writeLong(checksum);
                dataOutputStream.writeUTF(kvEntity.getKey());
                dataOutputStream.writeUTF(kvEntity.getVal());
            }

            // Perform a single I/O operation for performance
            raf.write(byteArrayOutputStream.toByteArray());
            raf.getFD().sync();
        } finally {
            lock.unlock();
        }
    }

    private long calculateChecksum(byte[] keyBytes, byte[] valBytes) {
        CRC32 crc32 = new CRC32();
        crc32.update(keyBytes);
        crc32.update(valBytes);
        return crc32.getValue();
    }

    public void write(String key, String val) {
        buffer.add(KVEntity.builder().key(key).val(val).build());
    }

    public void shutdown() {
        running = false;
        worker.interrupt(); // Stop the worker thread gracefully
    }


}
