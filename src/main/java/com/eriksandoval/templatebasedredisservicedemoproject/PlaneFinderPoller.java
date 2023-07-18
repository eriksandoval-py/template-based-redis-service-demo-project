package com.eriksandoval.templatebasedredisservicedemoproject;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Optional;

@EnableScheduling
@Component
public class PlaneFinderPoller {

    private WebClient client =
        WebClient.create("http://localhost:8080/aircraft");

    private final RedisConnectionFactory connectionFactory;
    private final RedisOperations<String, Aircraft> redisOperations;

    PlaneFinderPoller(RedisConnectionFactory connectionFactory,
              RedisOperations<String, Aircraft> redisOperations) {
        this.connectionFactory = connectionFactory;
        this.redisOperations = redisOperations;
    }

    @Scheduled(fixedRate = 1000) // Scheduled every 1 second
    public void pollPlanes() {
    connectionFactory.getConnection().serverCommands().flushDb();

    client.get()
        .retrieve()
        .bodyToFlux(Aircraft.class)
        .filter(plane -> !plane.getReg().isEmpty())
        .toStream()
        .forEach(ac -> redisOperations.opsForValue()
            .set(ac.getReg(), ac));

    Optional.ofNullable(redisOperations.opsForValue()
        .getOperations()
        .keys("*"))
        .ifPresent(keys -> keys.forEach(ac -> System.out.println(redisOperations.opsForValue().get(ac))));
    }

}
