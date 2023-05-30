package io.bsrevanth2011.github.graveldb.log;

import io.bsrevanth2011.github.graveldb.Entry;
import io.bsrevanth2011.github.graveldb.Index;
import io.bsrevanth2011.github.graveldb.db.DB;
import io.bsrevanth2011.github.graveldb.db.RocksDBService;
import org.eclipse.collections.api.factory.Lists;
import org.rocksdb.RocksDBException;

import java.util.List;

public class PersistentLog implements Log {

    private static final String LOG_METADATA = "logMetadata";

    private final DB<Index, Entry> db;
    private final DB<String, LogMetaData> metadataDB;
    private final LogMetaData logMetaData;

    public PersistentLog() throws RocksDBException {
        this.db = RocksDBService.getOrCreate("log", Index.class, Entry.class);
        this.metadataDB = RocksDBService.getOrCreate("logMetadata", String.class, LogMetaData.class);
        LogMetaData metadataSnapshot = metadataDB.get(LOG_METADATA);
        this.logMetaData = (metadataSnapshot != null) ? metadataSnapshot : new LogMetaData();
    }

    @Override
    public void appendEntry(int index, Entry entry) {
        db.put(intToIndex(index), entry);
        updateLogMetadata(index, entry.getTerm());
    }

    @Override
    public Entry getEntry(int index) {
        assert index > 0;
        return db.get(intToIndex(index));
    }

    private static Index intToIndex(int index) {
        return Index.newBuilder().setIndex(index).build();
    }

    @Override
    public Iterable<Entry> getEntriesInRange(int beginIndex, int endIndex) {
        assert beginIndex > 0;

        List<Entry> entries = Lists.mutable.of();
        for (int idx = beginIndex; idx <= endIndex; idx++) {
            entries.add(getEntry(idx));
        }

        return entries;
    }

    @Override
    public void deleteEntry(int index) {
        int lastLogIndex = Math.min(index - 1, getLastLogIndex());
        int lastLogTerm = lastLogIndex > 0 ? getEntry(lastLogIndex).getTerm() : 0;
        db.delete(intToIndex(index));
        updateLogMetadata(lastLogIndex, lastLogTerm);
    }

    @Override
    public int getLastLogIndex() {
        return logMetaData.getLastLogIndex();
    }

    @Override
    public int getLastLogTerm() {
        return logMetaData.getLastLogTerm();
    }

    private void updateLogMetadata(int index, int term) {
        logMetaData.setLastLogIndex(index);
        logMetaData.setLastLogTerm(term);
        metadataDB.put(LOG_METADATA, logMetaData);
    }
}
