package org.redis.wrapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.redis.util.LogHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
    public void close() {
        LOG.info("Closing JedisPool for prefix='{}'", prefix);
        jedisPool.close();
        LOG.info("JedisPool closed for prefix='{}'", prefix);
    }

    @Override
    public int size() {
        long t0 = System.nanoTime();
        try (Jedis jedis = jedisPool.getResource()) {
            Long result = jedis.scard(keysSetKey);
            int size = result.intValue();
            LOG.info("Size: keysSetKey='{}' -> {}", keysSetKey, size);
            LOG.debug("Size latencyMicros={}", (System.nanoTime() - t0) / 1_000);
            return size;
        } catch (Exception e) {
            LOG.error("Size check failed: keysSetKey='{}'", keysSetKey, e);
            throw e;
        }
    }

    @Override
    public boolean isEmpty() {
        boolean empty = size() == 0;
        LOG.debug("IsEmpty: {}", empty);
        return empty;
    }

    @Override
    public boolean containsKey(Object key) {
        @SuppressWarnings("unchecked")
        String serializedKey = serializeKey((K) key);
        long t0 = System.nanoTime();
        try (Jedis jedis = jedisPool.getResource()) {
            boolean result = jedis.sismember(keysSetKey, serializedKey);
            LOG.info("ContainsKey: key='{}' (serialized='{}') -> {}", LogHelper.safeToString(key), serializedKey, result);
            LOG.debug("ContainsKey latencyMicros={}", (System.nanoTime() - t0) / 1_000);
            return result;
        } catch (Exception e) {
            LOG.error("ContainsKey failed: key='{}'", LogHelper.safeToString(key), e);
            throw e;
        }
    }

    @Override
    public boolean containsValue(Object value) {
        @SuppressWarnings("unchecked")
        String serializedValue = serializeValue((V) value);
        long t0 = System.nanoTime();
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> allKeys = jedis.smembers(keysSetKey);
            LOG.debug("ContainsValue: keysSet size={}", allKeys.size());
            if (allKeys.isEmpty()) {
                LOG.info("ContainsValue: empty keysSet -> false");
                return false;
            }
            Pipeline pipeline = jedis.pipelined();
            List<Response<String>> responses = new ArrayList<>(allKeys.size());
            for (String key : allKeys) {
                responses.add(pipeline.get(keyForRedisString(key)));
            }
            pipeline.sync();
            int matchedIndex = -1;
            for (int i = 0; i < responses.size(); i++) {
                Response<String> response = responses.get(i);
                String stored = response.get();
                if (Objects.equals(stored, serializedValue)) {
                    matchedIndex = i;
                    break;
                }
            }
            boolean found = matchedIndex >= 0;
            LOG.info("ContainsValue: found={} (keysQueried={}, latencyMicros={})",
                    found,
                    responses.size(),
                    (System.nanoTime() - t0) / 1_000);
            return found;
        } catch (Exception e) {
            LOG.error("ContainsValue failed: value='{}'", LogHelper.safeToString(value), e);
            throw e;
        }
    }

    @Override
    public V get(Object o) {
        @SuppressWarnings("unchecked")
        String serializedKey = serializeKey((K) o);
        String redisKey = keyForRedisString(serializedKey);
        long t0 = System.nanoTime();
        try (Jedis jedis = jedisPool.getResource()) {
            String value = jedis.get(redisKey);
            if (value == null) {
                LOG.info("Get: MISS key='{}', redisKey='{}'", LogHelper.safeToString(o), redisKey);
                return null;
            }
            V val = deserializeValue(value);
            LOG.info("Get: HIT key='{}' -> valueType='{}', redisKey='{}'",
                    LogHelper.safeToString(o),
                    val != null ? val.getClass().getName() : "null", redisKey);
            LOG.debug("Get latencyMicros={}, valueJsonLength={}", (System.nanoTime() - t0) / 1_000, value.length());
            return val;
        } catch (Exception e) {
            LOG.error("Get failed: key='{}', redisKey='{}'", LogHelper.safeToString(o), redisKey, e);
            throw e;
        }
    }

    @Override
    public V put(K key, V value) {
        String serializedKey = serializeKey(key);
        String serializedValue = serializeValue(value);
        String redisKey = keyForRedisString(serializedKey);
        long t0 = System.nanoTime();

        try (Jedis jedis = jedisPool.getResource()) {
            String prev = jedis.get(redisKey);
            Pipeline pipeline = jedis.pipelined();
            pipeline.set(redisKey, serializedValue);
            pipeline.sadd(keysSetKey, serializedKey);
            pipeline.sync();
            LOG.info("Put: keyType='{}', valueType='{}', redisKey='{}', keysSetKey='{}'",
                    key.getClass().getName(),
                    value.getClass().getName(),
                    redisKey, keysSetKey);
            LOG.debug("Put latencyMicros={}, prevExists={}", (System.nanoTime() - t0) / 1_000, prev != null);

            if (prev == null) return null;

            return deserializeValue(prev);

        } catch (Exception e) {
            LOG.error("Put failed: key='{}', redisKey='{}'",
                    LogHelper.safeToString(key),
                    redisKey, e);
            throw e;
        }
    }

    @Override
    public V remove(Object key) {
        @SuppressWarnings("unchecked")
        String sk = serializeKey((K) key);
        String redisKeyForSerializedKey = keyForRedisString(sk);
        long t0 = System.nanoTime();

        try (Jedis jedis = jedisPool.getResource()) {
            String prev = jedis.get(redisKeyForSerializedKey);
            Pipeline pipeline = jedis.pipelined();
            pipeline.del(redisKeyForSerializedKey);
            pipeline.srem(keysSetKey, sk);
            pipeline.sync();
            LOG.info("Remove: key='{}', redisKey='{}', removedFromSet='{}'",
                    LogHelper.safeToString(key),
                    redisKeyForSerializedKey, keysSetKey);
            LOG.debug("Remove latencyMicros={}, hadPrev={}", (System.nanoTime() - t0) / 1_000, prev != null);

            if (prev == null) return null;
            return deserializeValue(prev);
        } catch (Exception e) {
            LOG.error("Remove failed: key='{}', redisKey='{}'",
                    LogHelper.safeToString(key),
                    redisKeyForSerializedKey, e);
            throw e;
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> another) {
        if (another.isEmpty()) {
            LOG.info("PutAll: empty map -> no-op");
            return;
        }
        long t0 = System.nanoTime();
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline pipeline = jedis.pipelined();
            int count = 0;
            for (Entry<? extends K, ? extends V> e : another.entrySet()) {
                String sk = serializeKey(e.getKey());
                String sv = serializeValue(e.getValue());
                pipeline.set(keyForRedisString(sk), sv);
                pipeline.sadd(keysSetKey, sk);
                count++;
            }
            pipeline.sync();
            LOG.info("PutAll: entries={}, keysSetKey='{}'", count, keysSetKey);
            LOG.debug("PutAll latencyMicros={}, pipelineOps={}", (System.nanoTime() - t0) / 1_000, count * 2);
        } catch (Exception e) {
            LOG.error("PutAll failed: entries={}", another.size(), e);
            throw e;
        }
    }

    @Override
    public void clear() {
        long t0 = System.nanoTime();
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> allSerializedKeys = jedis.smembers(keysSetKey);
            LOG.info("Clear: keysSetKey='{}', keysCount={}", keysSetKey, allSerializedKeys.size());
            if (!allSerializedKeys.isEmpty()) {
                Pipeline pipeline = jedis.pipelined();
                int ops = 0;
                for (String sk : allSerializedKeys) {
                    pipeline.del(keyForRedisString(sk));
                    ops++;
                }
                pipeline.del(keysSetKey);
                ops++;
                pipeline.sync();
                LOG.debug("Clear: pipelineOps={}, latencyMicros={}", ops, (System.nanoTime() - t0) / 1_000);
            } else {
                jedis.del(keysSetKey);
                LOG.debug("Clear: keysSetKey deleted (was already empty)");
            }
        } catch (Exception e) {
            LOG.error("Clear failed: keysSetKey='{}'", keysSetKey, e);
            throw e;
        }
    }

    @Override
    public Set<K> keySet() {
        long t0 = System.nanoTime();
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> ssk = jedis.smembers(keysSetKey);
            Set<K> result = new HashSet<>(ssk.size());
            for (String sk : ssk) {
                K k = deserializeKey(sk);
                result.add(k);
            }
            LOG.info("KeySet: size={}, keysSetKey='{}'", result.size(), keysSetKey);
            LOG.debug("KeySet latencyMicros={}", (System.nanoTime() - t0) / 1_000);

            return Collections.unmodifiableSet(result);

        } catch (Exception e) {
            LOG.error("KeySet failed: keysSetKey='{}'", keysSetKey, e);
            throw e;
        }
    }

    @Override
    public Collection<V> values() {
        long t0 = System.nanoTime();
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> ssk = jedis.smembers(keysSetKey);
            if (ssk.isEmpty()) {
                LOG.info("Values: empty -> []");
                return Collections.emptyList();
            }
            Pipeline pipeline = jedis.pipelined();
            List<Response<String>> responses = new ArrayList<>(ssk.size());
            for (String sk : ssk) {
                responses.add(pipeline.get(keyForRedisString(sk)));
            }
            pipeline.sync();
            List<V> values = new ArrayList<>(responses.size());
            int nullCount = 0;
            for (Response<String> r : responses) {
                String json = r.get();
                if (json == null) {
                    nullCount++;
                    values.add(null);
                } else {
                    V value = deserializeValue(json);
                    values.add(value);
                }
            }
            LOG.info("Values: count={}, nulls={}, latencyMicros={}",
                    values.size(),
                    nullCount,
                    (System.nanoTime() - t0) / 1_000);
            return Collections.unmodifiableList(values);
        } catch (Exception e) {
            LOG.error("Values failed", e);
            throw e;
        }
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        long t0 = System.nanoTime();
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> allKeys = jedis.smembers(keysSetKey);
            Set<Entry<K, V>> entries = new HashSet<>(allKeys.size());
            if (!allKeys.isEmpty()) {
                Pipeline pipeline = jedis.pipelined();
                Map<String, Response<String>> respMap = new LinkedHashMap<>();
                for (String key : allKeys) {
                    respMap.put(key, pipeline.get(keyForRedisString(key)));
                }
                pipeline.sync();
                int nullVals = 0;
                for (Map.Entry<String, Response<String>> e : respMap.entrySet()) {
                    String sk = e.getKey();
                    String jsonVal = e.getValue().get();
                    K k = deserializeKey(sk);
                    V v = jsonVal == null ? null : deserializeValue(jsonVal);
                    if (v == null) nullVals++;
                    entries.add(new AbstractMap.SimpleImmutableEntry<>(k, v));
                }
                LOG.info("EntrySet: size={}, nullValues={}, latencyMicros={}",
                        entries.size(),
                        nullVals,
                        (System.nanoTime() - t0) / 1_000);
            } else {
                LOG.info("EntrySet: empty");
            }
            return Collections.unmodifiableSet(entries);
        } catch (Exception e) {
            LOG.error("EntrySet failed: keysSetKey='{}'", keysSetKey, e);
            throw e;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Map)) return false;
        Map<?, ?> other = (Map<?, ?>) o;
        return this.entrySet().equals(other.entrySet());
    }

    @Override
    public int hashCode() {
        return entrySet().hashCode();
    }
}
