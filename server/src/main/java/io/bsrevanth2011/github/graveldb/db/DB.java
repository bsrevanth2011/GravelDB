package io.bsrevanth2011.github.graveldb.db;

public interface DB<K, V> {
    
    V get(K key);

    void put(K key, V value);

    void delete(K key);
}
