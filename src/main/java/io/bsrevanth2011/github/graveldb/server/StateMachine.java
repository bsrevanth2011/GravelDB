package io.bsrevanth2011.github.graveldb.server;

import io.bsrevanth2011.github.graveldb.Command;
import io.bsrevanth2011.github.graveldb.Key;
import io.bsrevanth2011.github.graveldb.KeyValuePair;
import io.bsrevanth2011.github.graveldb.Value;
import io.bsrevanth2011.github.graveldb.db.KVStore;

public class StateMachine {

    private final KVStore<Key, Value> db;

    public StateMachine(KVStore<Key, Value> db) {
        this.db = db;
    }

    public void commit(Command command) throws Exception {
        switch (command.getOp()) {
            case GET -> db.get(command.getData().getKey());
            case DELETE -> db.delete(command.getData().getKey());
            case PUT -> {
                KeyValuePair pair = command.getData().getKeyValuePair();
                db.put(pair.getKey(), pair.getValue());
            }
        }
    }

}
