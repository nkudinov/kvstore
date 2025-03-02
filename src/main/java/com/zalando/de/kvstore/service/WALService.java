package com.zalando.de.kvstore.service;


import com.zalando.de.kvstore.core.KVEntity;
import java.io.DataInputStream;
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
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WALService {

    public static final int MAGIC_NUMBER = 0xCAFEBABE;

    private static class WALEntity {

        String key;
        String val;

        public WALEntity(String key, String val) {
            this.key = key;
            this.val = val;
        }

        @Override
        public String toString() {
            return "WALEntity{" +
                "key='" + key + '\'' +
                ", val='" + val + '\'' +
                '}';
        }
    }

    private final String FILE_NAME = "wal.log";
    RandomAccessFile raf;
    Lock lock;

    public WALService() throws FileNotFoundException {
        raf = new RandomAccessFile(FILE_NAME, "rw");
        lock = new ReentrantLock();
    }

    public void write(KVEntity kvEntity) throws IOException {
        write(kvEntity.getKey(), kvEntity.getVal());
    }

    long calcCRC(byte[] a, byte[] b) {
        CRC32 crc32 = new CRC32();
        crc32.update(a);
        crc32.update(b);
        return crc32.getValue();
    }

    public void write(String key, String val) throws IOException {
        lock.lock();
        try {
            raf.seek(raf.length());
            raf.writeInt(MAGIC_NUMBER);
            raf.writeLong(calcCRC(key.getBytes(), val.getBytes()));
            raf.writeUTF(key);
            raf.writeUTF(val);
        } finally {
            lock.unlock();
        }
    }

    public boolean exists() {
        return Files.exists(Paths.get(FILE_NAME));
    }

    public List<KVEntity> recovery() throws IOException {
        lock.lock();
        try {
            log.info("recovery");
            List<KVEntity> res = new ArrayList<>();
            try (DataInputStream dis = new DataInputStream(new FileInputStream(FILE_NAME))) {
                log.info("to read: {}", dis.available());
                while (dis.available() > 0) {
                    int magicNumber = dis.readInt();
                    if (magicNumber != MAGIC_NUMBER) {
                        // skip corrupted record
                        continue;
                    }
                    long crc32 = dis.readLong();
                    String key = dis.readUTF();
                    String val = dis.readUTF();

                    if (crc32 != calcCRC(key.getBytes(), val.getBytes())) {
                        throw new IllegalStateException("val corrupted");
                    }
                    KVEntity kvEntity = KVEntity.builder().key(key).val(val).build();
                    log.info(kvEntity.toString());
                    res.add(kvEntity);
                }
            }
        } finally {
            lock.unlock();
        }
        return res;
    }
}
