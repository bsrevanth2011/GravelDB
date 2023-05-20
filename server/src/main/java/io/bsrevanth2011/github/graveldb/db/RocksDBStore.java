package io.bsrevanth2011.github.graveldb.db;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;


public class RocksDBStore<K extends com.google.protobuf.GeneratedMessageV3,
        V extends com.google.protobuf.GeneratedMessageV3> implements KVStore<K, V> {

    private final Logger logger = LoggerFactory.getLogger(RocksDBStore.class);
    private final String path;
    private final Class<V> vClass;
    private RocksDB db;

    static {
        RocksDB.loadLibrary();
    }

    public RocksDBStore(String path, Class<V> vClass) throws RocksDBException, IOException {
        this.path = path;
        this.vClass = vClass;
        init();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                db.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));
    }

    private void init() throws IOException, RocksDBException {
        Options options = new Options();
        options.setCreateIfMissing(true);

        File baseDir = new File(path);
        
        Files.createDirectories(baseDir.getParentFile().toPath());
        Files.createDirectories(baseDir.getAbsoluteFile().toPath());
        db = RocksDB.open(options, baseDir.getAbsolutePath());
        logger.info("RocksDB initialized");
    }

    @Override
    public V get(K key) throws RocksDBException {
        return deserializeValue(db.get(serializeKey(key)));
    }

    @Override
    public void delete(K key) throws RocksDBException {
        db.delete(serializeKey(key));
    }

    @Override
    public void put(K key, V value) throws RocksDBException {
        db.put(serializeKey(key), serializeValue(value));
    }

    private byte[] serializeKey(K key) {
        return key.toByteArray();
    }

    private byte[] serializeValue(V value) {
        return value.toByteArray();
    }

    @SuppressWarnings("unchecked")
    private V deserializeValue(byte[] bytes) {
        try {
            Method m = vClass.getMethod("parseFrom", byte[].class);
            return (V) m.invoke(null, (Object) bytes);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
