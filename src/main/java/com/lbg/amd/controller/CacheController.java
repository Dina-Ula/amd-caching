package com.lbg.amd.controller;

import com.lbg.amd.model.LargeRecord;
import com.lbg.amd.service.LargeRecordService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@RestController
public class CacheController {

    private final LargeRecordService largeRecordService;

    public CacheController(final LargeRecordService largeRecordService) {
        this.largeRecordService = largeRecordService;
    }

    @GetMapping("/getFromDB/{id}")
    public ResponseEntity<LargeRecord> getFromDBById(@PathVariable Long id) {

        final Optional<LargeRecord> largeRecord = largeRecordService.getFromDB(id);

        if (largeRecord.isPresent()) {
            final LargeRecord record = largeRecord.get();
            return ResponseEntity.ok(record);
        }

        return ResponseEntity.notFound().build();
    }

    @GetMapping("/getFromDB")
    public ResponseEntity<List<LargeRecord>> getFromDBByIds() {

        final Optional<List<LargeRecord>> largeRecord = largeRecordService.getFromDB(getListOfIdsToQuery(10000));

        if (largeRecord.isPresent()) {
            final List<LargeRecord> records = largeRecord.get();
            return ResponseEntity.ok(records);
        }

        return ResponseEntity.notFound().build();
    }

    @GetMapping("/getFromHotCache/{id}")
    public ResponseEntity<LargeRecord> getFromHotCache(@PathVariable Long id) {

        final Optional<LargeRecord> largeRecord = largeRecordService.getFromHotCache(id);

        if (largeRecord.isPresent()) {
            final LargeRecord record = largeRecord.get();
            return ResponseEntity.ok(record);
        }

        return ResponseEntity.notFound().build();
    }

    @GetMapping("/getFromHotCache")
    public ResponseEntity<List<LargeRecord>> getFromHotCache() {

        final Optional<List<LargeRecord>> largeRecord = largeRecordService.getFromHotCache(getListOfIdsToQuery(10000));

        if (largeRecord.isPresent()) {
            final List<LargeRecord> records = largeRecord.get();
            return ResponseEntity.ok(records);
        }

        return ResponseEntity.notFound().build();
    }

    @GetMapping("/getFromCache/{id}")
    public ResponseEntity<LargeRecord> getFromCache(@PathVariable Long id) {

        final Optional<LargeRecord> largeRecord = largeRecordService.getFromCache(id);

        if (largeRecord.isPresent()) {
            final LargeRecord record = largeRecord.get();
            return ResponseEntity.ok(record);
        }

        return ResponseEntity.notFound().build();
    }

    @GetMapping("/getFromCache")
    public ResponseEntity<List<LargeRecord>> getFromCache() {

        final Optional<List<LargeRecord>> largeRecord = largeRecordService.getFromCache(getListOfIdsToQuery(10000));

        if (largeRecord.isPresent()) {
            final List<LargeRecord> records = largeRecord.get();
            return ResponseEntity.ok(records);
        }

        return ResponseEntity.notFound().build();
    }

    private List<Long> getListOfIdsToQuery(int size) {

        List<Long> ids = new ArrayList<>();

        for (int i = 1; i <= size; i++) {
            ids.add((long) i);
        }

        return ids;
    }
}
