package io.bsrevanth2011.github.graveldb.db;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;


public class RocksDBService {

    private final Logger logger = LoggerFactory.getLogger(RocksDBService.class);

    private RocksDB db;

    static {
        RocksDB.loadLibrary();
    }

    public RocksDBService(String path) throws IOException, RocksDBException {
        init(path);
    }

    private void init(String path) throws IOException, RocksDBException {

        File baseDir = new File(path);

        if (!baseDir.exists()) {
            Files.createDirectories(baseDir.getParentFile().toPath());
            Files.createDirectories(baseDir.getAbsoluteFile().toPath());
        }

        Options options = new Options();
        options.setCreateIfMissing(true);

        db = RocksDB.open(options, baseDir.getAbsolutePath());
        logger.info("Initialized RocksDB with path := " + baseDir.getAbsoluteFile().toPath());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                db.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));
    }

    public byte[] get(byte[] key) {
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(byte[] key) {
        try {
            db.delete(key);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public void put(byte[] key, byte[] value) {
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
