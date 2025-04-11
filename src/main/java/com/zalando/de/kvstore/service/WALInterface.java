package com.zalando.de.kvstore.service;

import com.zalando.de.kvstore.core.KVEntity;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public interface WALInterface {

    Future<Void> write(String key, String val) throws IOException;

    void shutdown() throws IOException;

    boolean exists();

    List<KVEntity> recover() throws IOException;
}
