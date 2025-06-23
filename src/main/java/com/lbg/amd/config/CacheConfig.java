package com.lbg.amd.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.lbg.amd.model.LargeRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
public class CacheConfig {

    @Bean
    public Caffeine<Object, Object> caffeineConfig() {
        return Caffeine.newBuilder()
                .maximumSize(1_000_000)  // depending on memory size
                .expireAfterWrite(30, TimeUnit.MINUTES);
    }

    @Bean(name = "largeRecordCache")
    public Cache<Long, LargeRecord> largeRecordCache(Caffeine<Object, Object> caffeine) {
        return caffeine.build();
    }
}
