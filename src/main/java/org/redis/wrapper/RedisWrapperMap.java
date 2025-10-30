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

/**
 * Обёртка над Redis, реализующая интерфейс {@link Map}, с сериализацией ключей и значений в JSON.
 *
 * <p>Ключи и значения сериализуются с помощью {@link ObjectMapper} и хранятся в Redis как строки.
 * Для отслеживания всех ключей используется отдельное множество Redis ({@code keysSetKey}),
 * что позволяет реализовать методы {@code keySet()}, {@code values()},
 * {@code entrySet()} и {@code size()}.</p>
 *
 * <p>Ключи и значения сериализуются при записи и десериализуются при чтении. Типы {@code K} и {@code V}
 * определяются во время выполнения на основе переданных объектов. Все операции логируются с указанием
 * задержек и подробностей сериализации.</p>
 *
 * <p>Класс потокобезопасен при условии, что {@link JedisPool} не передаётся между потоками напрямую.</p>
 *
 * @param <K> тип ключей
 * @param <V> тип значений
 *
 * @see Map
 * @see JedisPool
 * @see ObjectMapper
 */
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

    /**
     * Формирует Redis-ключ для уже сериализованного ключа.
     *
     * <p>Комбинирует поле {@code prefix} с литералом {@code "entry"} и
     * переданной сериализованной строкой ключа, возвращая итоговый ключ для хранения в Redis(String).</p>
     *
     * @param key сериализованная строка ключа (не должна быть null)
     * @return итоговый Redis-ключ в формате {@code prefix + ":entry:" + key}
     */
    private String keyForRedisString(String key) {
        String redisKey = prefix + ":entry:" + key;
        if (LOG.isTraceEnabled()) {
            LOG.trace("Computed redisKeyForSerializedKey: serializedKey='{}' -> redisKey='{}'", key, redisKey);
        }
        return redisKey;
    }

    /**
     * Сериализует объект-ключ в JSON и сохраняет его runtime-класс в {@code keyType}.
     *
     * <p>Использует {@code objectMapper} для преобразования объекта в JSON.
     * Логирует информацию о сериализации на уровне DEBUG и пробрасывает ошибки как
     * непроверяемые исключения.</p>
     *
     * @param key объект-ключ для сериализации;
     * @return JSON-представление ключа
     * @throws RuntimeException если сериализация в JSON завершилась неудачей
     */
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

    /**
     * Сериализует объект-значение в JSON и сохраняет его runtime-класс в {@code valueType}.
     *
     * <p>Использует {@code objectMapper} для преобразования объекта в JSON.
     * Логирует информацию о сериализации на уровне DEBUG и пробрасывает ошибки как
     * непроверяемые исключения.</p>
     *
     * @param value объект-значение для сериализации; не должен быть null
     * @return JSON-представление значения
     * @throws RuntimeException если сериализация в JSON завершилась неудачей
     */
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

    /**
     * Десериализует JSON-строку в объект значения типа, сохранённого в {@code valueType}.
     *
     * <p>Если {@code json} равен {@code null}, возвращает {@code null}. Использует
     * {@code objectMapper} для чтения JSON в тип {@code valueType}. Логирует время
     * выполнения на уровне TRACE и пробрасывает ошибки как непроверяемые исключения.</p>
     *
     * @param json JSON-строка, представляющая значение, или {@code null}
     * @return десериализованное значение типа {@code V}, либо {@code null} если вход равен {@code null}
     * @throws RuntimeException если десериализация JSON завершилась неудачей
     */
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

    /**
     * Десериализует JSON-строку в объект ключа типа, сохранённого в {@code keyType}.
     *
     * <p>Если {@code json} равен {@code null}, возвращает {@code null}. Использует
     * {@code objectMapper} для чтения JSON в тип {@code keyType}. Логирует время
     * выполнения на уровне TRACE и пробрасывает ошибки как непроверяемые исключения.</p>
     *
     * @param json JSON-строка, представляющая ключ, или {@code null}
     * @return десериализованный ключ типа {@code K}, либо {@code null} если вход равен {@code null}
     * @throws RuntimeException если десериализация JSON завершилась неудачей
     */
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

    /**
     * Закрывает пул соединений Jedis.
     *
     * <p>Вызывается при завершении работы компонента, освобождая ресурсы Redis.</p>
     * <p>Логирует начало и завершение закрытия пула.</p>
     */
    @Override
    public void close() {
        LOG.info("Closing JedisPool for prefix='{}'", prefix);
        jedisPool.close();
        LOG.info("JedisPool closed for prefix='{}'", prefix);
    }

    /**
     * Возвращает количество элементов в хранилище.
     *
     * <p>Измеряет размер множества ключей {@code keysSetKey} в Redis с помощью команды {@code SCARD}.</p>
     * <p>Логирует результат и задержку выполнения.</p>
     *
     * @return количество элементов в хранилище
     * @throws RuntimeException если операция Redis завершилась ошибкой
     */
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

    /**
     * Проверяет, пусто ли хранилище.
     *
     * <p>Вызывает {@link #size()} и сравнивает результат с нулём.</p>
     *
     * @return {@code true}, если хранилище пусто; иначе {@code false}
     */
    @Override
    public boolean isEmpty() {
        boolean empty = size() == 0;
        LOG.debug("IsEmpty: {}", empty);
        return empty;
    }

    /**
     * Проверяет наличие указанного ключа в хранилище.
     *
     * <p>Сериализует ключ и проверяет его наличие в множестве {@code keysSetKey} с помощью {@code SISMEMBER}.</p>
     * <p>Логирует входные данные, результат и задержку выполнения.</p>
     *
     * @param key ключ для проверки
     * @return {@code true}, если ключ присутствует; иначе {@code false}
     * @throws RuntimeException если операция Redis завершилась ошибкой
     */
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

    /**
     * Проверяет наличие указанного значения в хранилище.
     *
     * <p>Сериализует значение и сравнивает его с каждым сохранённым значением
     * по всем ключам из {@code keysSetKey}.</p>
     * <p>Использует Redis Pipeline для параллельного получения значений.
     * Логирует размер множества, результат и задержку.</p>
     *
     * @param value значение для проверки
     * @return {@code true}, если значение найдено; иначе {@code false}
     * @throws RuntimeException если операция Redis завершилась ошибкой
     */
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

    /**
     * Получает значение, связанное с указанным ключом.
     *
     * <p>Сериализует ключ, формирует Redis-ключ и извлекает значение из Redis.
     * В случае отсутствия значения возвращает {@code null}. При наличии — десериализует
     * и возвращает объект типа {@code V}. Логирует попадание/промах и задержку выполнения.</p>
     *
     * @param o ключ, по которому производится поиск
     * @return значение, связанное с ключом, либо {@code null}, если оно отсутствует
     * @throws RuntimeException если операция Redis завершилась ошибкой
     */
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

    /**
     * Сохраняет значение по указанному ключу.
     *
     * <p>Сериализует ключ и значение, сохраняет их в Redis с помощью Pipeline.
     * Добавляет ключ в множество {@code keysSetKey}. Возвращает предыдущее значение,
     * если оно существовало, иначе {@code null}. Логирует типы, ключи и задержку.</p>
     *
     * @param key   ключ, по которому сохраняется значение
     * @param value значение для сохранения
     * @return предыдущее значение, если оно было, иначе {@code null}
     * @throws RuntimeException если операция Redis завершилась ошибкой
     */
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

    /**
     * Удаляет значение, связанное с указанным ключом.
     *
     * <p>Сериализует ключ, удаляет соответствующий Redis-ключ и исключает его из множества {@code keysSetKey}.
     * Возвращает предыдущее значение, если оно существовало, иначе {@code null}. Логирует удаление и задержку.</p>
     *
     * @param key ключ, по которому производится удаление
     * @return предыдущее значение, если оно было, иначе {@code null}
     * @throws RuntimeException если операция Redis завершилась ошибкой
     */
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

    /**
     * Добавляет все пары ключ-значение из переданной {@code Map<K, V>} в хранилище.
     *
     * <p>Сериализует каждый ключ и значение, сохраняет их в Redis и добавляет ключи в {@code keysSetKey}.
     * Использует Pipeline для оптимизации операций. Логирует количество записей и задержку.</p>
     *
     * @param another карта с данными для добавления
     * @throws RuntimeException если операция Redis завершилась ошибкой
     */
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

    /**
     * Очищает хранилище, удаляя все ключи и связанные значения.
     *
     * <p>Удаляет все Redis-ключи, соответствующие сериализованным ключам,
     * и само множество {@code keysSetKey}.
     * Использует Pipeline для пакетного удаления.
     * Логирует количество операций и задержку.</p>
     *
     * @throws RuntimeException если операция Redis завершилась ошибкой
     */
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

    /**
     * Возвращает множество всех ключей, присутствующих в хранилище.
     *
     * <p>Десериализует все элементы множества {@code keysSetKey}
     * и возвращает их как {@code Set<K>}.</p>
     *
     * @return неизменяемое множество ключей
     * @throws RuntimeException если операция Redis завершилась ошибкой
     */
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

    /**
     * Возвращает коллекцию всех значений, хранящихся в Redis.
     *
     * <p>Получает все значения по ключам из {@code keysSetKey}, десериализует их и возвращает как {@code List<V>}.</p>
     *
     * @return неизменяемая коллекция значений
     * @throws RuntimeException если операция Redis завершилась ошибкой
     */
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

    /**
     * Возвращает множество всех пар ключ-значение, хранящихся в Redis.
     *
     * <p>Получает все значения по ключам из {@code keysSetKey},
     * десериализует их и возвращает как {@code Set<Entry<K, V>>}.</p>
     *
     * @return неизменяемое множество пар ключ-значение
     * @throws RuntimeException если операция Redis завершилась ошибкой
     */
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
