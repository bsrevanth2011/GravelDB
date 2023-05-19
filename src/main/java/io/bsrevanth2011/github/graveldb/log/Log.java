package io.bsrevanth2011.github.graveldb.log;

import io.bsrevanth2011.github.graveldb.Entry;

public interface Log {

    void appendEntry(int index, Entry entry);

    void getLastLogTerm();

    void getLastLogIndex();

    void deleteLastEntry();

    Entry getEntry(int index);

    Entry getLastEntry();

    int getLastApplied(int consumerId);

}
