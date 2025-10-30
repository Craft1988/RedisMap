package org.redis.wrapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RedisWrapperMapTest {
    private JedisPool pool;
    private RedisWrapperMap<String, String> map;

    @BeforeAll
    public void beforeAll() {
        pool = new JedisPool(new JedisPoolConfig(), "localhost", 6379);
        map = new RedisWrapperMap<>(pool, "test_map");
        map.clear();
    }

    @AfterAll
    public void afterAll() {
        map.clear();
        map.close();
    }

    @BeforeEach
    public void beforeEach() {
        map.clear();
    }

    @Test
    public void testPutGetRemove() {
        assertTrue(map.isEmpty());
        assertNull(map.put("k1", "v1"));
        assertEquals("v1", map.get("k1"));
        assertEquals(1, map.size());
        assertEquals("v1", map.remove("k1"));
        assertNull(map.get("k1"));
        assertEquals(0, map.size());
    }

    @Test
    public void testContainsKeyAndValue() {
        map.put("a", "1");
        map.put("b", "2");
        assertTrue(map.containsKey("a"));
        assertTrue(map.containsValue("2"));
        assertFalse(map.containsValue("3"));
    }

    @Test
    public void testPutAllAndClear() {
        Map<String, String> other = new HashMap<>();
        other.put("x", "10");
        other.put("y", "20");
        map.putAll(other);
        assertEquals(2, map.size());
        assertEquals(Set.of("x", "y"), map.keySet());
        map.clear();
        assertTrue(map.isEmpty());
    }

    @Test
    public void testEntrySetAndValues() {
        map.put("k1", "v1");
        map.put("k2", "v2");
        Set<Map.Entry<String, String>> entries = map.entrySet();
        assertEquals(2, entries.size());
        Collection<String> vals = map.values();
        assertTrue(vals.contains("v1"));
        assertTrue(vals.contains("v2"));
    }

    @Test
    public void testRemoveViaKey() {
        map.put("a", "1");
        map.put("b", "2");

        map.remove("a");

        assertFalse(map.containsKey("a"));
        assertEquals(1, map.size());
    }
}