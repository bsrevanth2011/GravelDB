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
import java.lang.reflect.ParameterizedType;
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

    @SuppressWarnings("unchecked")
    public RocksDBStore(String path) {
        this.path = path;
        this.vClass = (Class<V>) ((ParameterizedType) getClass()
                .getGenericSuperclass())
                .getActualTypeArguments()[1];
    }

    private void init() {
        Options options = new Options();
        options.setCreateIfMissing(true);

        File baseDir = new File(path);
        try {
            Files.createDirectories(baseDir.getParentFile().toPath());
            Files.createDirectories(baseDir.getAbsoluteFile().toPath());
            db = RocksDB.open(options, baseDir.getAbsolutePath());
            logger.info("RocksDB initialized");
        } catch (IOException | RocksDBException e) {
            logger.error("Error initializing RocksDB. Exception: ", e);
        }
    }

    @Override
    public V get(K key) throws Exception {
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
