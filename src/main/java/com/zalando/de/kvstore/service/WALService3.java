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
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

public class WALService3 implements WALInterface {

    public static final int RECORD_BEGIN = 0xDEADDEAD;
    private static String FILE_NAME = "wal.log";
    private RandomAccessFile raf;

    private BlockingQueue<KVEntity> buffer;

    private Lock lock;

    private Thread worker;

    private AtomicBoolean isRunning;

    public WALService3() throws IOException {
        raf = new RandomAccessFile(FILE_NAME, "rw");
        raf.seek(raf.length());
        lock = new ReentrantLock();
        buffer = new LinkedBlockingQueue<>();
        isRunning = new AtomicBoolean(true);
        worker = new Thread() {
            @Override
            public void run() {
                while (isRunning.get()) {
                    KVEntity kvEntity = null;
                    try {
                        kvEntity = buffer.take();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    try {
                        assert kvEntity != null;
                        internalWrite(kvEntity.getKey(), kvEntity.getVal());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
        worker.start();
    }

    private long crcSum(String key, String val) {
        CRC32 crc32 = new CRC32();
        crc32.update(key.getBytes());
        crc32.update(val.getBytes());
        return crc32.getValue();
    }

    @Override
    public Future<Void> write(String key, String val) throws IOException {
        buffer.add(KVEntity.builder().key(key).val(val).build());
        return new CompletableFuture<>();
    }


    public void internalWrite(String key, String val) throws IOException {
        lock.lock();
        try {
            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
                dataOutputStream.write(RECORD_BEGIN);
                dataOutputStream.writeLong(crcSum(key, val));
                dataOutputStream.writeUTF(key);
                dataOutputStream.writeUTF(val);
                raf.write(byteArrayOutputStream.toByteArray());
                raf.getFD().sync();
            }
        } finally {
            lock.unlock();

        }
    }

    @Override
    public void shutdown() throws IOException {
        raf.getFD().sync();
        raf.close();
    }

    @Override
    public boolean exists() {
        return Files.exists(Paths.get(FILE_NAME));
    }

    @Override
    public List<KVEntity> recover() throws IOException {
        List<KVEntity> res = new ArrayList<>();
        try (DataInputStream dis = new DataInputStream(new FileInputStream(FILE_NAME))) {
            while (true) {
                if (dis.readInt() != RECORD_BEGIN) {
                    continue;
                }
                long crc32 = dis.readLong();
                String key = dis.readUTF();
                String val = dis.readUTF();
                res.add(KVEntity.builder().key(key).val(val).build());
            }
        } catch (EOFException e) {

        }
        return res;
    }
}
