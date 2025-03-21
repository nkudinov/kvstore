package com.zalando.de.kvstore.service;

import com.zalando.de.kvstore.core.KVEntity;
import java.io.IOException;
import java.util.List;

public interface WALInterface {

    void write(String key, String val) throws IOException;

    void shutdown() throws IOException;

    boolean exists();

    List<KVEntity> recover() throws IOException;
}
