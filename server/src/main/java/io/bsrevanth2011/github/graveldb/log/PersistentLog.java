package io.bsrevanth2011.github.graveldb.log;

import com.google.protobuf.InvalidProtocolBufferException;
import io.bsrevanth2011.github.graveldb.Entry;
import io.bsrevanth2011.github.graveldb.db.RocksDBService;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class PersistentLog implements Log {

    private static final byte[] LAST_LOG_INDEX_KEY = "lastLogIndex".getBytes(Charset.defaultCharset());
    private static final byte[] LAST_LOG_TERM_KEY = "lastLogTerm".getBytes(Charset.defaultCharset());

    private final RocksDBService log;
    private final RocksDBService logMetadata;
    private int lastLogIndex;
    private int lastLogTerm;

    public PersistentLog(String logDir, String logMetadataDir) throws RocksDBException, IOException {
        this.log = new RocksDBService(logDir);
        this.logMetadata = new RocksDBService(logMetadataDir);
    }

    @Override
    public void appendEntry(int index, Entry entry) {
        log.put(intToBytes(index), entry.toByteArray());
        updateLogMetadata(index, entry.getTerm());
    }

    @Override
    public Entry getEntry(int index) {
        if (index == 0) return null;
        try {
            return Entry.parseFrom(log.get(intToBytes(index)));
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] intToBytes(int index) {
        return ByteBuffer.allocate(4).putInt(index).array();
    }

    @Override
    public Iterable<Entry> getEntriesInRange(int beginIndex, int endIndex) {
        List<Entry> entries = new ArrayList<>(endIndex - beginIndex + 1);
        for (int idx = beginIndex; idx <= endIndex; idx++) {
            entries.add(getEntry(idx));
        }

        return entries;
    }

    @Override
    public void deleteEntry(int index) {
        int lastLogIndex = index - 1;
        int lastLogTerm = lastLogIndex > 0 ? getEntry(lastLogIndex).getTerm() : 0;
        log.delete(intToBytes(index));
        updateLogMetadata(lastLogIndex, lastLogTerm);
    }

    @Override
    public int getLastLogIndex() {
        return lastLogIndex;
    }

    @Override
    public int getLastLogTerm() {
        return lastLogTerm;
    }

    private void updateLogMetadata(int index, int term) {
        this.lastLogIndex = index;
        this.lastLogTerm = term;
        logMetadata.put(LAST_LOG_INDEX_KEY, intToBytes(index));
        logMetadata.put(LAST_LOG_TERM_KEY, intToBytes(term));
    }
}
