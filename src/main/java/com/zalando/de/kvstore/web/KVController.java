package com.zalando.de.kvstore.web;


import com.zalando.de.kvstore.core.KVEntity;
import com.zalando.de.kvstore.core.KVStore;
import com.zalando.de.kvstore.service.WALInterface;
import com.zalando.de.kvstore.service.WALService;
import com.zalando.de.kvstore.service.WALService2;
import com.zalando.de.kvstore.service.WALService3;
import com.zalando.de.kvstore.service.WALService5;
import com.zalando.de.kvstore.service.WALService7;
import com.zalando.de.kvstore.service.WALService8;
import com.zalando.de.kvstore.service.WalService11;
import com.zalando.de.kvstore.service.WalService12;
import com.zalando.de.kvstore.service.WalService6;
import com.zalando.de.kvstore.service.WalService9;
import com.zalando.de.kvstore.snapshot.SnapshotService8;
import com.zalando.de.kvstore.snapshot.SnapshotService9;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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

    private Lock lock = new ReentrantLock();
    private KVStore store = new KVStore();
    private WALInterface wal = new WalService12(lock);
    private SnapshotService9 snapshotService8 = new SnapshotService9(lock, wal, store);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public KVController() throws IOException {
        if (wal.exists()) {
            for (KVEntity entity : wal.recover()) {
                store.put(entity);
            }
        }
        Runnable task = () -> {
            try {
                snapshotService8.takeSnapshot();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

        long initialDelay = 0;
        long period = 30;
        scheduler.scheduleAtFixedRate(task, initialDelay, period, TimeUnit.MINUTES);
    }

    @GetMapping("/{key}")
    ResponseEntity<String> get(@PathVariable String key) {
        Optional<String> val = store.get(key);
        return val.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PutMapping("/{key}")
    ResponseEntity<String> put(@PathVariable String key, @RequestBody String val)
        throws IOException, ExecutionException, InterruptedException {
        KVEntity kvEntity = KVEntity.builder().key(key).val(val).build();
        var future = wal.write(kvEntity.getKey(), kvEntity.getVal());
        store.put(kvEntity);
        future.get();
        return ResponseEntity.ok("ack");
    }
}
