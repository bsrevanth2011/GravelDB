package io.bsrevanth2011.github.graveldb.log;

import io.bsrevanth2011.github.graveldb.Entry;
import org.rocksdb.RocksDBException;

public interface Log {

    void appendEntry(int index, Entry entry);

    int getLastLogTerm();

    int getLastLogIndex();

    void deleteLastEntry();

    Entry getEntry(int index);

    Entry getLastEntry();

    int getLastApplied();

}
