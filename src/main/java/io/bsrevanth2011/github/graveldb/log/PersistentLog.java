package io.bsrevanth2011.github.graveldb.log;

import io.bsrevanth2011.github.graveldb.Entry;

public class PersistentLog implements Log {


    @Override
    public void appendEntry(int index, Entry entry) {

    }

    @Override
    public void getLastLogTerm() {

    }

    @Override
    public void getLastLogIndex() {

    }

    @Override
    public void deleteLastEntry() {

    }

    @Override
    public Entry getEntry(int index) {
        return null;
    }

    @Override
    public Entry getLastEntry() {
        return null;
    }

    @Override
    public int getLastApplied(int consumerId) {
        return 0;
    }
}
