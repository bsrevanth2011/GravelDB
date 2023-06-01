package io.bsrevanth2011.github.graveldb.db;

import io.bsrevanth2011.github.graveldb.LeaderInfo;

public interface DB<K, V> {
    
    V get(K key);

    void put(K key, V value);

    void delete(K key);

    default LeaderInfo getLeaderInfo() { throw new UnsupportedOperationException(); }

    default boolean isLeader() { throw new UnsupportedOperationException(); }
}
