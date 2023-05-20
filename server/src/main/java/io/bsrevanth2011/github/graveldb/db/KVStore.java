package io.bsrevanth2011.github.graveldb.db;

public interface KVStore<K, V> {

    V get(K key);

    void delete(K key);

    void put(K key, V value);

}
