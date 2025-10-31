package org.redis;

import org.redis.model.User;
import org.redis.wrapper.RedisWrapperMap;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.UUID;

public class Main {
    public static void main(String[] args) {
        JedisPool jedis = new JedisPool("localhost", 6379);

        User u1 = new User("u1", "Mike", List.of(1, 2, 3, 3));
        User u2 = new User("u2", "Joe", List.of(1, 2, 3, 3));
        User u3 = new User("u3", "Julia", List.of(1, 2, 3, 3));
        UUID u1Id = UUID.randomUUID();
        UUID u2Id = UUID.randomUUID();
        UUID u3Id = UUID.randomUUID();

        RedisWrapperMap<UUID, User> map = new RedisWrapperMap<>(jedis, "users");

        map.put(u1Id, u1);
        map.put(u2Id, u2);
        map.put(u3Id, u3);

        map.clear();

        map.close();

    }
}