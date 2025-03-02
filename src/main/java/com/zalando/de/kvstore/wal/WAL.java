package com.zalando.de.kvstore.wal;

import com.zalando.de.kvstore.core.KVEntity;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WAL {

    private String WAL_FILE_NAME = "wal.log";

    public void write(KVEntity entity) throws IOException {
        Path path = Paths.get(WAL_FILE_NAME);
        Files.write(path, entity.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public boolean exists() {
        return Files.exists(Paths.get(WAL_FILE_NAME));
    }

    private Set<KVEntity> read(int offset) {
        Set<KVEntity> res = new HashSet<>();
        try (RandomAccessFile file = new RandomAccessFile(WAL_FILE_NAME, "r")) {
            file.seek(offset);

            while (file.getFilePointer() < file.length()) {
                // Read key length as integer
                int kLen = readLength(file);
                if (file.getFilePointer() >= file.length()) break;

                // Read value length as integer
                int vLen = readLength(file);
                if (file.getFilePointer() >= file.length()) break;

                // Read key using UTF-8 encoding
                String key = readString(file, kLen);
                if (file.getFilePointer() >= file.length()) break;

                // Read value using UTF-8 encoding
                String value = readString(file, vLen);

                // Add the entity to the result set
                res.add(KVEntity.builder().key(key).val(value).build());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    // Helper method to read length values (key and value lengths)
    private int readLength(RandomAccessFile file) throws IOException {
        int length = 0;
        byte b;
        while (file.getFilePointer() < file.length() && (b = file.readByte()) != ':') {
            length = length * 10 + (b - '0');
        }
        return length;
    }

    // Helper method to read a UTF-8 encoded string of a given length in bytes
    private String readString(RandomAccessFile file, int length) throws IOException {
        byte[] buffer = new byte[length];
        int bytesRead = file.read(buffer);
        if (bytesRead != length) {
            throw new IOException("Unexpected end of file while reading UTF-8 string");
        }
        return new String(buffer, StandardCharsets.UTF_8);
    }

    public Set<KVEntity> readAll() {
        return read(0);
    }
}
