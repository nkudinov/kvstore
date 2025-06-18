package com.zalando.de.kvstore.service;

import com.zalando.de.kvstore.core.KVEntity;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;

public class WalService15 implements WALInterface {

    public static final String WAL_LOG = "wal.log";

    private final FileChannel fileChannel;

    private final Lock lock;

    public WalService15(Lock lock) {
        this.lock = lock;
        fileChannel = FileChannel.open(Paths.get(WAL_LOG), StandardOpenOption.WRITE, StandardOpenOption.SYNC);
    }

    @Override
    public boolean exists() {
        return Files.exists(Path.of(WAL_LOG));
    }

    @Override
    public Future<Void> write(String key, String val) throws IOException {
        return null;
    }

    @Override
    public void shutdown() throws IOException {

    }

    @Override
    public List<KVEntity> recover() throws IOException {
        return List.of();
    }

    @Override
    public long offset() throws IOException {
        lock.lock();
        try {
            return fileChannel.position();
        } finally {
            lock.unlock();
        }
    }
}
