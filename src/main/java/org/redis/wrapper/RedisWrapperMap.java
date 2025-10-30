package org.redis.wrapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.redis.util.LogHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.*;

public class RedisWrapperMap<K, V> implements Map<K, V>, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(RedisWrapperMap.class);
    private final JedisPool jedisPool;
    private final ObjectMapper objectMapper;
    private final String prefix;
    private final String keysSetKey;
    private Class<V> valueType;
    private Class<K> keyType;

    public RedisWrapperMap(JedisPool pool, String prefix) {
        this.jedisPool = Objects.requireNonNull(pool, "the 'pool' refers to null!");
        this.objectMapper = new ObjectMapper();
        this.prefix = Objects.requireNonNull(prefix, "prefix is null!");
        this.keysSetKey = prefix + ":keys";
        LOG.info("Initialized RedisWrapperMap: prefix='{}', keysSetKey='{}'", this.prefix, this.keysSetKey);
    }

    private String keyForRedisString(String key) {
        String redisKey = prefix + ":entry:" + key;
        if (LOG.isTraceEnabled()) {
            LOG.trace("Computed redisKeyForSerializedKey: serializedKey='{}' -> redisKey='{}'", key, redisKey);
        }
        return redisKey;
    }

    private String serializeKey(K key) {
        keyType = (Class<K>) key.getClass();
        long t0 = System.nanoTime();
        try {
            String json = objectMapper.writeValueAsString(key);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Serialized key: type='{}', jsonLength={}, json='{}', timeMicros={}",
                        keyType.getName(),
                        json.length(), json, (System.nanoTime() - t0) / 1_000);
            }
            return json;
        } catch (JsonProcessingException e) {
            LOG.error("Failed to serialize key: type='{}', value='{}'", keyType, LogHelper.safeToString(key), e);
            throw new RuntimeException("Unable to serialize key!", e);
        }
    }

    private String serializeValue(V value) {
        valueType = (Class<V>) value.getClass();
        long t0 = System.nanoTime();
        try {
            String json = objectMapper.writeValueAsString(value);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Serialized value: type='{}', jsonLength={}, timeMicros={}",
                        valueType.getName(),
                        json.length(), (System.nanoTime() - t0) / 1_000);
            }
            return json;
        } catch (JsonProcessingException e) {
            LOG.error("Failed to serialize value: type='{}', value='{}'",
                    valueType,
                    LogHelper.safeToString(value), e);
            throw new RuntimeException("Unable to serialize value!", e);
        }
    }

    private V deserializeValue(String json) {
        if (json == null) return null;
        long t0 = System.nanoTime();
        try {
            V value = objectMapper.readValue(json, valueType);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Deserialized value (generic): jsonLength={}, timeMicros={}",
                        json.length(),
                        (System.nanoTime() - t0) / 1_000);
            }
            return value;
        } catch (IOException e) {
            LOG.error("Failed to deserialize value: jsonLength={}, json='{}'",
                    json.length(),
                    LogHelper.truncate(json, 512), e);
            throw new RuntimeException("Unable to deserialize value", e);
        }
    }

    private K deserializeKey(String json) {
        if (json == null) return null;
        long t0 = System.nanoTime();
        try {
            K key = objectMapper.readValue(json, keyType);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Deserialized: targetType='{}', jsonLength={}, timeMicros={}",
                        keyType != null ? keyType.getName() : "null", json.length(),
                        (System.nanoTime() - t0) / 1_000);
            }
            return key;
        } catch (IOException e) {
            LOG.error("Failed to deserialize: targetType='{}', jsonLength={}, json='{}'",
                    keyType != null ? keyType.getName() : "null", json.length(),
                    LogHelper.truncate(json, 512), e);
            throw new RuntimeException("Unable to deserialize", e);
        }
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object o) {
        return false;
    }

    @Override
    public boolean containsValue(Object o) {
        return false;
    }

    @Override
    public V get(Object o) {
        return null;
    }

    @Override
    public V put(K k, V v) {
        return null;
    }

    @Override
    public V remove(Object o) {
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {

    }

    @Override
    public void clear() {

    }

    @Override
    public Set<K> keySet() {
        return Set.of();
    }

    @Override
    public Collection<V> values() {
        return List.of();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return Set.of();
    }
}
