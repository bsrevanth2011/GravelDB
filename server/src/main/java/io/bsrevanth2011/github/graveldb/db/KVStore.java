package io.bsrevanth2011.github.graveldb.db;

public interface KVStore<K, V> {

    V get(K key) throws Exception;

    void delete(K key) throws Exception;

    void put(K key, V value) throws Exception;

}
