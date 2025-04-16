package com.zalando.de.kvstore.service;

import com.zalando.de.kvstore.core.KVEntity;
import com.zalando.de.kvstore.service.WALInterface;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class WALService7 implements WALInterface {

    public static final String WAL_LOG = "wal.log";
    public static final int MAGIC_HEADER = 0xBEBEBEBE;
    private final AtomicBoolean isRunning;
    private Lock lock;

    private static class WALRecord {

        protected final String key;
        protected final String val;

        private final CompletableFuture<Void> future;

        public WALRecord(String key, String val) {
            this.key = key;
            this.val = val;
            this.future = new CompletableFuture<>();
        }
    }

    private final BlockingQueue<WALRecord> buffer;
    private final RandomAccessFile raf;
    private Thread worker;

    public WALService7() throws IOException {
        this.buffer = new LinkedBlockingQueue<>();
        this.isRunning = new AtomicBoolean(true);
        this.raf = new RandomAccessFile(WAL_LOG, "rw");
        raf.seek(raf.length());
        this.lock = new ReentrantLock();
        this.worker = new Thread() {
            @Override
            public void run() {
                while (isRunning.get()) {
                    try {
                       log.info("waiting");
                        WALRecord walRecord = buffer.take();
                        processRecords(List.of(walRecord));
                        walRecord.future.complete(null);
                    } catch (InterruptedException e) {

                            Thread.currentThread().interrupt();

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
        this.worker.start();
    }

    private void processRecords(List<WALRecord> walRecord) throws IOException {
        log.info("processing..");
        lock.lock();
        try {
            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
                for (WALRecord record : walRecord) {
                    dataOutputStream.writeInt(MAGIC_HEADER);
                    dataOutputStream.writeLong(crc32(record.key.getBytes(), record.val.getBytes()));
                    dataOutputStream.writeUTF(record.key);
                    dataOutputStream.writeUTF(record.val);
                }
                raf.write(byteArrayOutputStream.toByteArray());
                raf.getFD().sync();
            }
        } finally {
            lock.unlock();
        }
    }

    private long crc32(byte[] b1, byte[] b2) {
        CRC32 crc32 = new CRC32();
        crc32.update(b1);
        crc32.update(b2);
        return crc32.getValue();
    }

    @Override
    public Future<Void> write(String key, String val) throws IOException {
        WALRecord walRecord = new WALRecord(key, val);
        buffer.add(walRecord);
        log.info("added to queue");
        return walRecord.future;
    }

    @Override
    public void shutdown() throws IOException {
        isRunning.set(false);
        worker.interrupt();
        try {
            worker.join();
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
        if (!exists()) {
            return new ArrayList<>();
        }
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(WAL_LOG)))) {
            List<KVEntity> res = new ArrayList<>();
            try {
                while (true) {
                    if (in.readInt() != MAGIC_HEADER) {
                        continue;
                    }
                    long crc = in.readLong();
                    String key = in.readUTF();
                    String val = in.readUTF();
                    if (crc != crc32(key.getBytes(), val.getBytes())) {
                        continue;
                    }
                    res.add(KVEntity.builder().key(key).val(val).build());
                }
            } catch (EOFException e) {

            }
            return res;
        } catch (EOFException e) {
            throw new RuntimeException(e);
        }
    }

}
