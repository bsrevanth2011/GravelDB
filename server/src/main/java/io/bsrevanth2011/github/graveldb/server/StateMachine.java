package io.bsrevanth2011.github.graveldb.server;

import io.bsrevanth2011.github.graveldb.Key;
import io.bsrevanth2011.github.graveldb.Value;
import io.bsrevanth2011.github.graveldb.db.DB;
import io.bsrevanth2011.github.graveldb.db.RocksDBService;
import org.rocksdb.RocksDBException;

public class StateMachine {

    private final DB<Key, Value> db;

    public StateMachine() throws RocksDBException {
        this.db = RocksDBService.getOrCreate("data", Key.class, Value.class);
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
