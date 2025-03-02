package com.zalando.de.kvstore.core;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class KVStore {

    private final ConcurrentHashMap<String, String> store;

    public KVStore() {
        store = new ConcurrentHashMap<>();

    }

    public String put(String key, String value) {
        return store.put(key, value);
    }

    public String put(KVEntity kvEntity) {
        return store.put(kvEntity.getKey(), kvEntity.getVal());
    }

    public Optional<String> get(String key) {
        return Optional.ofNullable(store.get(key));
    }

    public Optional<KVEntity> getEntity(String key) {
        return Optional.ofNullable(store.get(key))
            .map(value -> KVEntity.builder().key(key).val(value).build());
    }
}
