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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WalService13 implements WALInterface {

    public static final String WAL_LOG = "wal.log";
    public static final int MAGIC_NUMBER = 0XCAFEBEBE;

    private class WALEntry {

        private String key;
        private String val;
        private CompletableFuture<Void> future;

        public WALEntry(String key, String val) {
            this.key = key;
            this.val = val;
            this.future = new CompletableFuture<>();
        }
    }

    private BlockingQueue<WALEntry> buffer;
    private AtomicBoolean isRunning;

    private final Lock lock;

    private final RandomAccessFile raf;

    private class Worker extends Thread {

        @Override
        public void run() {
            while (isRunning.get() || !buffer.isEmpty()) {
                WALEntry walEntry = null;
                try {
                    walEntry = buffer.poll(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (walEntry != null) {
                    try {
                        write(walEntry);
                    } catch (IOException e) {
                        walEntry.future.completeExceptionally(e);
                        throw new RuntimeException(e);
                    }
                    walEntry.future.complete(null);
                }
            }

        }
    }

    private final Worker worker;

    private void write(WALEntry walEntry) throws IOException {
        lock.lock();
        try {
            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
                dataOutputStream.writeInt(MAGIC_NUMBER);
                dataOutputStream.writeLong(crc32(walEntry.key, walEntry.val));
                dataOutputStream.writeUTF(walEntry.key);
                dataOutputStream.writeUTF(walEntry.val);

                raf.write(byteArrayOutputStream.toByteArray());
                raf.getFD().sync();
            }
        } finally {
            lock.unlock();
        }
    }

    private long crc32(String... strs) {
        CRC32 crc32 = new CRC32();
        for (String s : strs) {
            crc32.update(s.getBytes());
        }
        return crc32.getValue();
    }

    public WalService13() throws FileNotFoundException {
        buffer = new ArrayBlockingQueue<>(1000);
        isRunning = new AtomicBoolean(true);
        raf = new RandomAccessFile(WAL_LOG, "rw");
        lock = new ReentrantLock();
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
        List<KVEntity> res = new ArrayList<>();
        try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(WAL_LOG))) {
            try {
                while (true) {
                    if (dataInputStream.readInt() != MAGIC_NUMBER) {
                        log.info("skip till next record");
                        continue;
                    }
                    long crc = dataInputStream.readLong();
                    String key = dataInputStream.readUTF();
                    String val = dataInputStream.readUTF();
                    if (crc32(key, val) != crc) {
                        log.info("skip record");
                        continue;
                    }
                    res.add(KVEntity.builder().key(key).val(val).build());
                }
            } catch (EOFException eofException) {

            }
        }
        return res;
    }

    @Override
    public long offset() throws IOException {
        lock.lock();
        try {
            return raf.getFilePointer();
        } finally {
            lock.unlock();
        }
    }
}
