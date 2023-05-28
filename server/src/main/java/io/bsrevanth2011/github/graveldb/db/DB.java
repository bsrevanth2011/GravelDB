package io.bsrevanth2011.github.graveldb.db;

public interface DB<K, V, R> {
    R get(K key) throws Exception;

    R put(K key, V value) throws Exception;

    R delete(K key) throws Exception;

    boolean isMaster();
}
