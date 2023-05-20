package io.bsrevanth2011.github.graveldb.server;

import io.bsrevanth2011.github.graveldb.Key;
import io.bsrevanth2011.github.graveldb.Value;
import io.bsrevanth2011.github.graveldb.db.KVStore;

public class StateMachine {

    private final KVStore<Key, Value> db;

    public StateMachine(KVStore<Key, Value> db) {
        this.db = db;
    }

    public Value get(Key key) {
        return db.get(key);
    }

    public void put(Key key, Value value) {
        db.put(key, value);
    }

    public void delete(Key key) {
        db.delete(key);
    }

}
