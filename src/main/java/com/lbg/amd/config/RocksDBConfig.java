package com.lbg.amd.config;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RocksDBConfig {

    private static RocksDB rocksDB;

    static {
        RocksDB.loadLibrary();
    }

    @Bean(destroyMethod = "close")
    public RocksDB rocksDB() throws RocksDBException {
        Options options = new Options().setCreateIfMissing(true);
        rocksDB = RocksDB.open(options, "/Users/dina/Learning/amd-caching/data");
        return rocksDB;
    }
}
