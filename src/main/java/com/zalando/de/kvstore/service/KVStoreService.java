package com.zalando.de.kvstore.service;

import com.zalando.de.kvstore.core.KVStore;
import com.zalando.de.kvstore.wal.WAL;
import java.io.IOException;

public class KVStoreService {

    KVStore kvStore;

    WALService walService;

    public KVStoreService(KVStore kvStore, WALService walService) {
        this.kvStore = kvStore;
        this.walService = walService;
    }

    public String put(String key, String value) throws IOException {
        walService.write(key,value);
        return kvStore.put(key, value);
    }
}
