package io.bsrevanth2011.github.graveldb.db;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.GeneratedMessageV3;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Maps;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

public class RocksDBService {

    private static final Logger logger = LoggerFactory.getLogger(RocksDBService.class);

    private static RocksDB db;
    private static final Map<String, ColumnFamilyHandle> dbMap = Maps.mutable.of();
    private static final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
    private static final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();

    public static void init(String path) throws IOException, RocksDBException {

        RocksDB.loadLibrary();

        File baseDir = new File(path);

        if (!baseDir.exists()) {
            Files.createDirectories(baseDir.getParentFile().toPath());
            Files.createDirectories(baseDir.getAbsoluteFile().toPath());
        }

        Options options = new Options();
        List<ColumnFamilyDescriptor> cfDescriptors = Lists.mutable.of();

        cfDescriptors.addAll(RocksDB
                .listColumnFamilies(options, baseDir.getAbsolutePath())
                .stream()
                .map(nameBytes -> new ColumnFamilyDescriptor(nameBytes, cfOptions))
                .toList());

        if (cfDescriptors.isEmpty()) {
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
        }

        List<ColumnFamilyHandle> cfHandles = Lists.mutable.of();
        db = RocksDB.open(dbOptions, baseDir.getAbsolutePath(),
                cfDescriptors, cfHandles);

        logger.debug("Initialized RocksDB with baseDir := " + baseDir.getAbsoluteFile().toPath());

        for (int i = 0; i < cfDescriptors.size(); i++) {
            String cfName = new String(cfDescriptors.get(i).getName());
            dbMap.put(cfName, cfHandles.get(i));
        }

        Runtime.getRuntime().addShutdownHook(new Thread(RocksDBService::close));
    }

    public static <K, V> DB<K, V> getOrCreate(String name, Class<K> kClass, Class<V> vClass)
            throws RocksDBException {

        createColumnFamilyIfAbsent(name);
        return new RocksDatabase<>(dbMap.get(name), kClass, vClass);
    }

    public static <K, V> DB<K, V> getOrCreate(String name,
                                              Serializer<K> keySerializer,
                                              Serializer<V> valueSerializer,
                                              Deserializer<V> valueDeserializer)
            throws RocksDBException {

        createColumnFamilyIfAbsent(name);
        return new RocksDatabase<>(dbMap.get(name), keySerializer, valueSerializer, valueDeserializer);
    }

    private static void createColumnFamilyIfAbsent(String name) throws RocksDBException {
        if (!dbMap.containsKey(name)) {
            byte[] nameBytes = name.getBytes(Charset.defaultCharset());
            ColumnFamilyDescriptor newCfDescriptor = new ColumnFamilyDescriptor(nameBytes, cfOptions);
            ColumnFamilyHandle newCfHandle = db.createColumnFamily(newCfDescriptor);
            dbMap.put(name, newCfHandle);
        }
    }

    private static void close() {
        try {
            db.cancelAllBackgroundWork(true);
            for (var handle : dbMap.values()) {
                handle.close();
            }
            db.syncWal();
            db.closeE();
            dbOptions.close();
            cfOptions.close();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private static final class RocksDatabase<K, V> implements DB<K, V> {

        private final ColumnFamilyHandle cfHandle;
        private final Serializer<K> keySerializer;
        private final Serializer<V> valueSerializer;
        private final Deserializer<V> valueDeserializer;

        public RocksDatabase(ColumnFamilyHandle cfHandle, Class<K> kClass, Class<V> vClass) {
            assert vClass != null;

            if (TypeToken.of(kClass).isSubtypeOf(TypeToken.of(GeneratedMessageV3.class))) {
                this.keySerializer = protoSerializer();
            } else if (TypeToken.of(kClass).isSubtypeOf(TypeToken.of(Serializable.class))) {
                this.keySerializer = standardSerializer();
            } else {
                throw new IllegalArgumentException("Key and Value classes should either both implement Serializable or GeneratedMessageV3, " +
                        "or the user needs to provide serializer for key class and both serializer and deserializer for value class");
            }

            if (TypeToken.of(kClass).isSubtypeOf(TypeToken.of(GeneratedMessageV3.class))) {
                this.valueSerializer = protoSerializer();
                this.valueDeserializer = protoDeserializer(vClass);
            } else if (TypeToken.of(kClass).isSubtypeOf(TypeToken.of(Serializable.class))) {
                this.valueSerializer = standardSerializer();
                this.valueDeserializer = standardDeserializer();
            } else {
                throw new IllegalArgumentException("Key and Value classes should either both implement Serializable or GeneratedMessageV3, " +
                        "or the user needs to provide serializer for key class and both serializer and deserializer for value class");
            }

            this.cfHandle = cfHandle;
        }

        private <T> Deserializer<T> standardDeserializer() {
            return bytes -> {
                try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
                    try (ObjectInputStream ois = new ObjectInputStream(bais)) {
                        //noinspection unchecked
                        return (T) ois.readObject();
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
        }

        private <T> Serializer<T> standardSerializer() {
            return obj -> {
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                    try (ObjectOutputStream oas = new ObjectOutputStream(baos)) {
                        oas.writeObject(obj);
                        oas.flush();
                        return baos.toByteArray();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
        }

        private static <T> Deserializer<T> protoDeserializer(Class<T> vClass) {
            return (bytes) -> {
                try {
                    //noinspection unchecked
                    return (T) vClass.getDeclaredMethod("parseFrom", byte[].class).invoke(null, (Object) bytes);
                } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }
            };
        }

        private static <T> Serializer<T> protoSerializer() {
            return (key) -> ((GeneratedMessageV3) key).toByteArray();
        }

        public RocksDatabase(ColumnFamilyHandle cfHandle,
                             Serializer<K> keySerializer,
                             Serializer<V> valueSerializer,
                             Deserializer<V> valueDeserializer) {

            this.cfHandle = cfHandle;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.valueDeserializer = valueDeserializer;
        }

        @Override
        public V get(K key) {
            try {
                byte[] valueBytes = db.get(cfHandle, serializeKey(key));
                return valueBytes != null ? deserializeValue(valueBytes) : null;
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void delete(K key) {
            try {
                db.delete(cfHandle, serializeKey(key));
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void put(K key, V value) {
            try {
                db.put(cfHandle, serializeKey(key), serializeValue(value));
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }

        private byte[] serializeKey(K key) {
            return keySerializer.serialize(key);
        }

        private byte[] serializeValue(V value) {
            return valueSerializer.serialize(value);
        }

        private V deserializeValue(byte[] valueBytes) {
            return valueDeserializer.deserialize(valueBytes);
        }

    }
}
