package io.bsrevanth2011.github.graveldb.server;

import com.google.protobuf.InvalidProtocolBufferException;
import io.bsrevanth2011.github.graveldb.Key;
import io.bsrevanth2011.github.graveldb.Value;
import io.bsrevanth2011.github.graveldb.db.RocksDBService;

public class StateMachine {

    private final RocksDBService db;

    public StateMachine(RocksDBService db) {
        this.db = db;
    }

    public Value get(Key key) {
        try {
            return Value.parseFrom(db.get(key.toByteArray()));
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public void put(Key key, Value value) {
        db.put(key.getKey().toByteArray(), value.getValue().toByteArray());
    }

    public void delete(Key key) {
        db.delete(key.getKey().toByteArray());
    }

}
