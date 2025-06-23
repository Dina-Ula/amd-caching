package com.lbg.amd.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.lbg.amd.model.LargeRecord;
import jakarta.annotation.PostConstruct;
import org.rocksdb.RocksDB;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

@Service
public class LargeRecordService {

    private final JdbcTemplate jdbcTemplate;
    private final Executor executor;

    private final RocksDB rocksDB;

    private final ObjectMapper objectMapper;

    private final Cache<Long, LargeRecord> largeRecordCache;

    private static final int TOTAL = 20_000_000;
    private static final int BATCH_SIZE = 1_000_000;

    public LargeRecordService(final ObjectMapper objectMapper, final RocksDB rocksDB, JdbcTemplate jdbcTemplate, @Qualifier("recordExecutor") Executor executor, @Qualifier("largeRecordCache") Cache<Long, LargeRecord> largeRecordCache) {
        this.rocksDB = rocksDB;
        this.jdbcTemplate = jdbcTemplate;
        this.executor = executor;
        this.largeRecordCache = largeRecordCache;
        this.objectMapper = new ObjectMapper();
    }

    @PostConstruct
    public void loadCacheInParallel() throws InterruptedException {

        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();

        System.out.printf("Initial Heap: %d MB%n", heapUsage.getInit() / (1024 * 1024));
        System.out.printf("Used Heap: %d MB%n", heapUsage.getUsed() / (1024 * 1024));
        System.out.printf("Max Heap: %d MB%n", heapUsage.getMax() / (1024 * 1024));
        System.out.printf("Committed Heap: %d MB%n", heapUsage.getCommitted() / (1024 * 1024));

        long startTime = System.currentTimeMillis();
        System.out.println("Start time: " + (startTime) / 1000 + " seconds");

        List<CompletableFuture<Void>> tasks = new ArrayList<>();

        for (int i = 0; i < TOTAL; i += BATCH_SIZE) {
            final int start = i + 1;
            final int end = i + BATCH_SIZE;

            System.out.println("Batch: " + i);

            CompletableFuture<Void> task = CompletableFuture.runAsync(() -> fetchBatch(start, end), executor);
            tasks.add(task);
        }

        CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();

        long endTime = System.currentTimeMillis();
        System.out.println("End time: " + (endTime) / 1000 + " seconds");

        System.out.println("Completed in: " + (endTime - startTime) / 1000 + " seconds");
    }

    private List<LargeRecord> fetchBatch(int start, int end) {
        String sql = "SELECT id, name, email, description, created_at FROM large_table WHERE id BETWEEN ? AND ?";
        jdbcTemplate.setFetchSize(1000); // Optional tuning
        List<LargeRecord> records = jdbcTemplate.query(sql, new Object[]{start, end}, new BeanPropertyRowMapper<>(LargeRecord.class));
        System.out.println("Fetched batch: " + start + " to " + end + " -> " + records.size() + " records");
        return records;
    }


    private void loadCache(List<LargeRecord> records, int start, int end) {
        for (LargeRecord record : records) {
            try {
                byte[] key = ByteBuffer.allocate(Long.BYTES).putLong(record.getId()).array();
                byte[] value = objectMapper.writeValueAsBytes(record);
                rocksDB.put(key, value);
            } catch (Exception e) {
                throw new RuntimeException("RocksDB put failed", e);
            }
        }

        System.out.printf("Cached records: %d to %d%n", start, end);
    }

    public Optional<LargeRecord> getFromDB(Long id) {
        String sql = "SELECT id, name, email, description, created_at FROM large_table WHERE id = ?";
        List<LargeRecord> records = jdbcTemplate.query(sql, new Object[]{id}, new BeanPropertyRowMapper<>(LargeRecord.class));
        System.out.println("Fetched id: " + id + ", size: " + records.size());

        if (records.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(records.get(0));
    }

    public Optional<List<LargeRecord>> getFromDB(List<Long> ids) {

        List<LargeRecord> largeRecords = new ArrayList<>();

        for (Long id : ids) {
            String sql = "SELECT id, name, email, description, created_at FROM large_table WHERE id = ?";
            List<LargeRecord> records = jdbcTemplate.query(sql, new Object[]{id}, new BeanPropertyRowMapper<>(LargeRecord.class));
            System.out.println("Fetched id: " + id + ", size: " + records.size());

            largeRecords.add(records.get(0));
        }

        if (largeRecords.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(largeRecords);
    }

    public Optional<LargeRecord> getFromHotCache(Long id) {

        LargeRecord record = largeRecordCache.getIfPresent(id);

        if (record != null) {
            return Optional.of(record);
        }

        // Fallback to RocksDB
        try {
            byte[] key = ByteBuffer.allocate(Long.BYTES).putLong(id).array();

            byte[] value = rocksDB.get(key);
            if (value != null) {
                LargeRecord deserialized = objectMapper.readValue(value, LargeRecord.class);
                largeRecordCache.put(id, deserialized); // cache hot again
                return Optional.of(deserialized);
            }
        } catch (Exception e) {
            throw new RuntimeException("RocksDB get failed", e);
        }

        return Optional.empty();
    }

    public Optional<List<LargeRecord>> getFromHotCache(List<Long> ids) {
        List<LargeRecord> largeRecords = new ArrayList<>();

        for (Long id : ids) {
            LargeRecord largeRecord = largeRecordCache.getIfPresent(id);

            if (largeRecord != null) {
                largeRecords.add(largeRecord);
                continue;
            }

            // Fallback to RocksDB
            try {
                byte[] key = ByteBuffer.allocate(Long.BYTES).putLong(id).array();

                byte[] value = rocksDB.get(key);
                if (value != null) {
                    largeRecord = objectMapper.readValue(value, LargeRecord.class);
                    largeRecordCache.put(id, largeRecord); // cache hot again
                    largeRecords.add(largeRecord);
                }
            } catch (Exception e) {
                throw new RuntimeException("RocksDB get failed", e);
            }
        }

        if (largeRecords.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(largeRecords);
    }

    public Optional<LargeRecord> getFromCache(Long id) {
        try {
            byte[] key = ByteBuffer.allocate(Long.BYTES).putLong(id).array();

            byte[] value = rocksDB.get(key);
            if (value != null) {
                LargeRecord largeRecord = objectMapper.readValue(value, LargeRecord.class);
                return Optional.of(largeRecord);
            }
        } catch (Exception e) {
            throw new RuntimeException("RocksDB get failed", e);
        }

        return Optional.empty();
    }

    public Optional<List<LargeRecord>> getFromCache(List<Long> ids) {

        List<LargeRecord> largeRecords = new ArrayList<>();

        for (Long id : ids) {
            try {
                byte[] key = ByteBuffer.allocate(Long.BYTES).putLong(id).array();

                byte[] value = rocksDB.get(key);
                if (value != null) {
                    LargeRecord largeRecord = objectMapper.readValue(value, LargeRecord.class);
                    largeRecords.add(largeRecord);
                }
            } catch (Exception e) {
                throw new RuntimeException("RocksDB get failed", e);
            }
        }

        if (largeRecords.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(largeRecords);
    }
}