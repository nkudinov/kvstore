package com.zalando.de.kvstore.service;

import com.zalando.de.kvstore.core.KVEntity;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class WalService6 implements WALInterface {

    private static final Logger logger = LoggerFactory.getLogger(WalService6.class);

    private static class WALRecord {

        final String key;
        final String val;
        final CompletableFuture<Void> future = new CompletableFuture<>();

        WALRecord(String key, String val) {
            this.key = key;
            this.val = val;
        }
    }

    public static final int MAGIC_NUMBER = 0xBEBEBEBE;
    public static final String WAL_LOG = "wal.log";

    private final RandomAccessFile raf;
    private final Lock writeLock = new ReentrantLock();

    private final BlockingQueue<WALRecord> queue = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread worker;

    public WalService6() throws IOException {
        this.raf = new RandomAccessFile(WAL_LOG, "rw");
        this.raf.seek(raf.length());

        this.worker = new Thread(this::runWorker, "wal-writer-thread");
        this.worker.start();
        logger.info("WAL service initialized.");
    }

    private void runWorker() {
        while (running.get() || !queue.isEmpty()) {
            WALRecord record = null;
            try {
                record = queue.poll(100, TimeUnit.MILLISECONDS);
                if (record == null) {
                    continue;
                }

                writeToDisk(record);
                record.future.complete(null);
            } catch (Exception e) {
                if (record != null) {
                    record.future.completeExceptionally(e);
                }
                logger.error("Failed to write WAL record", e);
            }
        }

        logger.info("WAL writer thread stopped.");
    }

    @Override
    public Future<Void> write(String key, String val) throws IOException {
        WALRecord record = new WALRecord(key, val);
        queue.offer(record);
        try {
            return record.future;
        } catch (CompletionException   e) {
            throw new IOException("WAL write failed", e.getCause());
        }
    }

    private void writeToDisk(WALRecord record) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(MAGIC_NUMBER);
            dos.writeLong(crc32(record.key.getBytes(), record.val.getBytes()));
            dos.writeUTF(record.key);
            dos.writeUTF(record.val);
        }

        writeLock.lock();
        try {
            raf.write(baos.toByteArray());
            raf.getFD().sync();
        } finally {
            writeLock.unlock();
        }
    }

    private long crc32(byte[] keyBytes, byte[] valBytes) {
        CRC32 crc = new CRC32();
        crc.update(keyBytes);
        crc.update(valBytes);
        return crc.getValue();
    }

    @Override
    public void shutdown() throws IOException {
        logger.info("Shutting down WAL service...");
        running.set(false);
        try {
            worker.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while waiting for WAL worker to stop");
        }

        writeLock.lock();
        try {
            raf.close();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean exists() {
        return Files.exists(Paths.get(WAL_LOG));
    }

    @Override
    public List<KVEntity> recover() throws IOException {
        List<KVEntity> result = new ArrayList<>();

        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(WAL_LOG)))) {
            while (true) {
                try {
                    int magic = in.readInt();
                    if (magic != MAGIC_NUMBER) {
                        continue;
                    }

                    long crc = in.readLong();
                    String key = in.readUTF();
                    String val = in.readUTF();

                    if (crc == crc32(key.getBytes(), val.getBytes())) {
                        result.add(KVEntity.builder().key(key).val(val).build());
                    } else {
                        logger.warn("Corrupt WAL record skipped: key='{}'", key);
                    }
                } catch (EOFException e) {
                    break;
                }
            }
        }

        logger.info("WAL recovery completed with {} records.", result.size());
        return result;
    }
    @Override
    public long offset() throws IOException {
        return raf.getFilePointer();
    }
}
