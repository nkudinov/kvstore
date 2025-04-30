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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.zip.CRC32;

public class WALService5 implements WALInterface {

    public static final String WAL_LOG = "wal.log";
    public static final int MAGIC_NUMBER = 0XBEBEBEBE;
    private BlockingQueue<KVEntity> buffer;
    private Thread writer;
    private AtomicBoolean isRunning;

    private RandomAccessFile raf;

    private WALService5() throws FileNotFoundException {
        buffer = new LinkedBlockingQueue<>();
        raf = new RandomAccessFile(WAL_LOG, "rw");
        writer = new Thread() {
            @Override
            public void run() {
                while (isRunning.get()) {
                    try {
                        KVEntity kvEntity = buffer.take();
                        writeToLog(kvEntity);
                    } catch (InterruptedException | IOException e) {
                        if (!isRunning.get()) {
                            throw new RuntimeException(e);
                        }
                        Thread.currentThread().interrupt();
                    }
                }
            }
        };
    }

    private void writeToLog(KVEntity kvEntity) throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
            dataOutputStream.writeInt(MAGIC_NUMBER);
            dataOutputStream.writeLong(crc32(kvEntity.getKey().getBytes(), kvEntity.getVal().getBytes()));
            dataOutputStream.writeUTF(kvEntity.getKey());
            dataOutputStream.writeUTF(kvEntity.getVal());
            raf.write(byteArrayOutputStream.toByteArray());
            raf.getFD().sync();
        }
    }

    private long crc32(byte[] bytes, byte[] bytes1) {
        CRC32 crc32 = new CRC32();
        crc32.update(bytes);
        crc32.update(bytes1);
        return crc32.getValue();
    }

    private static volatile WALInterface instnace;

    static public WALInterface getInstance() throws FileNotFoundException {
        if (instnace == null) {
            synchronized (WALService5.class) {
                if (instnace == null) {
                    instnace = new WALService5();
                }
            }
        }
        return instnace;
    }

    @Override
    public Future<Void> write(String key, String val) throws IOException {
        buffer.add(KVEntity.builder().key(key).val(val).build());
        return new CompletableFuture<>();
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
        return Files.exists(Path.of(WAL_LOG));
    }

    @Override
    public List<KVEntity> recover() throws IOException {
        List<KVEntity> res = new ArrayList<>();
        try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(WAL_LOG))) {
            while (true) {
                int magicNumber = dataInputStream.readInt();
                if (magicNumber != MAGIC_NUMBER) {
                    continue;
                }
                long rowCRC = dataInputStream.readLong();
                String key = dataInputStream.readUTF();
                String val = dataInputStream.readUTF();
                if (rowCRC != crc32(key.getBytes(), val.getBytes())) {
                    continue;
                }
                res.add(KVEntity.builder().key(key).val(val).build());
            }

        } catch (EOFException e) {

        }
        return res;
    }
    @Override
    public long offset() throws IOException {
        return raf.getFilePointer();
    }
}
