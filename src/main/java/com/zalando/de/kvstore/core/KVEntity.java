package com.zalando.de.kvstore.core;

import java.nio.charset.StandardCharsets;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class KVEntity {

    private String key;
    private String val;

    public byte[] getBytes() {
        StringBuilder sb = new StringBuilder();
        sb.append(key.length())
            .append(':')
            .append(val.length())
            .append(':')
            .append(key)
            .append(val);
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }


}
