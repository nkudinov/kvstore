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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.zip.CRC32;
import lombok.SneakyThrows;

public class WALService8 implements WALInterface {

    private static final int MAGIC_NUMBER = 0xCAFEBEBE;
    private static final int POLL_TIMEOUT_MS = 100;

    private final Path walFilePath;
    private final Lock writeLock;
    private final BlockingQueue<WALEntry> entryQueue;
    private final AtomicBoolean isRunning;
    private final RandomAccessFile logFile;
    private final Thread logWorkerThread;

    public WALService8(Lock writeLock, String logFileName) throws FileNotFoundException {
        this.writeLock = writeLock;
        this.walFilePath = Path.of(logFileName);
        this.entryQueue = new LinkedBlockingQueue<>();
        this.isRunning = new AtomicBoolean(true);
        this.logFile = new RandomAccessFile(walFilePath.toFile(), "rw");

        this.logWorkerThread = new Thread(new LogWorker(), "wal-log-writer");
        this.logWorkerThread.start();
    }

    @Override
    public Future<Void> write(String key, String value) {
        WALEntry entry = new WALEntry(key, value);
        entryQueue.add(entry);
        return entry.future;
    }

    @Override
    public void shutdown() throws IOException {
        isRunning.set(false);
        logWorkerThread.interrupt();
        try {
            logWorkerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while shutting down WAL service", e);
        } finally {
            logFile.close();
        }
    }

    @Override
    public boolean exists() {
        return Files.exists(walFilePath);
    }

    @Override
    public List<KVEntity> recover() throws IOException {
        List<KVEntity> entries = new ArrayList<>();

        try (DataInputStream input = new DataInputStream(
            new BufferedInputStream(new FileInputStream(walFilePath.toFile())))) {

            while (true) {
                int magic = input.readInt();
                if (magic != MAGIC_NUMBER) continue;

                long crc = input.readLong();
                String key = input.readUTF();
                String value = input.readUTF();

                if (calculateCRC(key, value) == crc) {
                    entries.add(KVEntity.builder().key(key).val(value).build());
                }
            }
        } catch (EOFException eof) {
            // End of file reached — normal termination
        }

        return entries;
    }


    private class LogWorker implements Runnable {
        @Override
        public void run() {
            try {
                logFile.seek(logFile.length());

                while (isRunning.get() || !entryQueue.isEmpty()) {
                    WALEntry entry = entryQueue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    if (entry != null) {
                        writeLogEntry(entry);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Unexpected WAL worker failure", e);
            }
        }

        private void writeLogEntry(WALEntry entry) {
            try {
                writeLock.lock();
                writeEntryToFile(entry);
                entry.future.complete(null);
            } catch (Exception e) {
                entry.future.completeExceptionally(e);
            } finally {
                writeLock.unlock();
            }
        }

        private void writeEntryToFile(WALEntry entry) throws IOException {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(buffer);

            output.writeInt(MAGIC_NUMBER);
            output.writeLong(calculateCRC(entry.key, entry.value));
            output.writeUTF(entry.key);
            output.writeUTF(entry.value);

            logFile.write(buffer.toByteArray());
            logFile.getFD().sync();
        }
    }

    private static class WALEntry {
        final String key;
        final String value;
        final CompletableFuture<Void> future = new CompletableFuture<>();

        WALEntry(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    private static long calculateCRC(String key, String value) {
        CRC32 crc = new CRC32();
        crc.update(key.getBytes());
        crc.update(value.getBytes());
        return crc.getValue();
    }
    @Override
    public long offset() throws IOException {
        return logFile.getFilePointer();
    }
}
