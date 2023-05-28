package io.bsrevanth2011.github.graveldb.log;

import io.bsrevanth2011.github.graveldb.Entry;

public interface Log {

    void appendEntry(int index, Entry entry);

    Entry getEntry(int index);

    Iterable<Entry> getEntriesInRange(int beginIndex, int endIndex);

    void deleteEntry(int index);

    int getLastLogIndex();

    int getLastLogTerm();
}
