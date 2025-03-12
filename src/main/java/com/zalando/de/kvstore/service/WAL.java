package com.zalando.de.kvstore.service;

import com.zalando.de.kvstore.core.KVEntity;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class WAL {

    private static int MAGIC_NUMBER = 0xCAFEBABE;
    private BlockingQueue<KVEntity> buffer;
    Lock lock;
    Thread worker;
    RandomAccessFile raf;

    public WAL() {
        this.buffer = new LinkedBlockingQueue<>();
        this.lock = new ReentrantLock();
        String s = "wal.log";
        this.raf = new RandomAccessFile("wal.log", "rw");
        worker = new Thread() {
            @Override
            public void run() {
                while (true) {
                    List<KVEntity> list = new ArrayList<>();
                    try {
                        var entity = buffer.poll(10, TimeUnit.MICROSECONDS);
                        if (entity != null) {
                            list.add(entity);
                        }
                        if (entity == null || list.size() == 10) {
                            write(list);
                        }

                    } catch (InterruptedException | IOException e) {
                        throw new RuntimeException(e);
                    }

                }
            }
        };
        worker.start();
    }

    private void write(List<KVEntity> list) throws IOException {
        lock.lock();
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
            for (KVEntity kvEntity : list) {
                dataOutputStream.write(MAGIC_NUMBER);
                dataOutputStream.writeUTF(kvEntity.getKey());
                dataOutputStream.writeUTF(kvEntity.getVal());
            }
            raf.write(byteArrayOutputStream.toByteArray());
            raf.getFD().sync();
        } finally {
            lock.unlock();
        }
    }

    public void write(String key, String val) {
        buffer.add(KVEntity.builder().key(key).val(val).build());
    }

}
