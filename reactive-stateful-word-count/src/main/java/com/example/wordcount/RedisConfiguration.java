package com.example.wordcount;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.reactive.RedisStringReactiveCommands;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedisConfiguration {

    @Bean
    public RedisStringReactiveCommands<String,String> reactiveCommands(RedisClient client) {
        return client.connect().reactive();
    }

    @Bean
    public RedisClient redisClient() {
        return RedisClient.create("redis://localhost");
    }
}
