package com.zalando.de.kvstore.service;

import com.zalando.de.kvstore.core.KVEntity;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

public class WALService {

    private static final String FILE_NAME = "wal.log";
    RandomAccessFile raf;
    Lock lock;

    public WALService() throws IOException {
        this.raf = new RandomAccessFile(FILE_NAME, "rw");
        raf.seek(raf.length());
        this.lock = new ReentrantLock();
    }

    public boolean exists() {
        return Files.exists(Paths.get(FILE_NAME));
    }

    private static int MAGIC_NUMBER = 0xCAFEBABE;

    private long checksum(byte[] keyBytes, byte[] valBytes) {
        CRC32 crc32 = new CRC32();
        crc32.update(keyBytes);
        crc32.update(valBytes);
        return crc32.getValue();
    }


    public void write(KVEntity kvEntity) throws IOException {
        write(kvEntity.getKey(), kvEntity.getVal());
    }

    public void write(String key, String value) throws IOException {
        lock.lock();
        try {
            raf.writeInt(MAGIC_NUMBER);
            raf.writeLong(checksum(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8)));
            raf.writeUTF(key);
            raf.writeUTF(value);
            raf.getFD().sync();
        } finally {
            lock.unlock();
        }
    }

    public List<KVEntity> recovery() throws IOException {
        List<KVEntity> res = new ArrayList<>();
        lock.lock();
        try (DataInputStream dis = new DataInputStream(new FileInputStream(FILE_NAME))) {
            while (dis.available() != 0) {
                int magicNumber = dis.readInt();
                if (magicNumber != MAGIC_NUMBER) {
                    continue;
                }
                long crc = dis.readLong();
                String key = dis.readUTF();
                String val = dis.readUTF();
                KVEntity kvEntity = KVEntity.builder().key(key).val(val).build();
                res.add(kvEntity);
            }
        } finally {
            lock.unlock();
        }
        return res;
    }
}
