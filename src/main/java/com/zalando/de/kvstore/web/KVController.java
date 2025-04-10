package com.zalando.de.kvstore.web;


import com.zalando.de.kvstore.core.KVEntity;
import com.zalando.de.kvstore.core.KVStore;
import com.zalando.de.kvstore.service.WALInterface;
import com.zalando.de.kvstore.service.WALService;
import com.zalando.de.kvstore.service.WALService2;
import com.zalando.de.kvstore.service.WALService3;
import com.zalando.de.kvstore.service.WALService5;
import com.zalando.de.kvstore.service.WalService6;
import java.io.IOException;
import java.util.Optional;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/kv")
public class KVController {

    private KVStore store = new KVStore();
    private WALInterface wal = new WalService6();

    public KVController() throws IOException {
        if (wal.exists()) {
            for (KVEntity entity : wal.recover()) {
                store.put(entity);
            }
        }

    }

    @GetMapping("/{key}")
    ResponseEntity<String> get(@PathVariable String key) {
        Optional<String> val = store.get(key);
        return val.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PutMapping("/{key}")
    ResponseEntity<String> put(@PathVariable String key, @RequestBody String val) throws IOException {
        KVEntity kvEntity = KVEntity.builder().key(key).val(val).build();
        wal.write(kvEntity.getKey(), kvEntity.getVal());
        store.put(kvEntity);
        return ResponseEntity.ok("ack");
    }
}
