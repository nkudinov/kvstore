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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

public class WalService6 implements WALInterface {


    public static final int MAGIC_NUMBER = 0xBEBEBEBE;
    Lock lock;
    RandomAccessFile raf;

    public WalService6() throws IOException {
        lock = new ReentrantLock();
        raf = new RandomAccessFile(WAL_LOG, "rw");
        raf.seek(raf.length());
    }

    public static final String WAL_LOG = "wal.log";

    @Override
    public void write(String key, String val) throws IOException {
        lock.lock();
        try {
            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
                dataOutputStream.writeInt(MAGIC_NUMBER);
                dataOutputStream.writeLong(crc32(key.getBytes(), val.getBytes()));
                dataOutputStream.writeUTF(key);
                dataOutputStream.writeUTF(val);
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
    public void shutdown() throws IOException {
        raf.close();
    }

    @Override
    public boolean exists() {
        return Files.exists(Paths.get(WAL_LOG));
    }

    @Override
    public List<KVEntity> recover() throws IOException {
        List<KVEntity> res = new ArrayList<>();
        try (DataInputStream bufferedInputStream = new DataInputStream(new FileInputStream(WAL_LOG))) {
            while (true) {
                try {
                    if (bufferedInputStream.readInt() != MAGIC_NUMBER) {
                        continue;
                    }
                    long crc = bufferedInputStream.readLong();
                    String key = bufferedInputStream.readUTF();
                    String val = bufferedInputStream.readUTF();
                    if (crc != crc32(key.getBytes(), val.getBytes())) {
                        continue;
                    }
                    res.add(KVEntity.builder().key(key).val(val).build());
                } catch (EOFException e) {
                    break;
                }
            }

        }
        return res;
    }
}
