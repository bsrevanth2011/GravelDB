package io.bsrevanth2011.github.graveldb.log;

import io.bsrevanth2011.github.graveldb.Entry;
import io.bsrevanth2011.github.graveldb.Index;
import io.bsrevanth2011.github.graveldb.db.KVStore;
import io.bsrevanth2011.github.graveldb.db.RocksDBStore;
import org.rocksdb.RocksDBException;

import java.io.*;

public class PersistentLog implements Log {

    private final KVStore<Index, Entry> log;
    private final LogMetadata logMetadata;
    public PersistentLog(String logDir, String logMetadataDir) throws RocksDBException, IOException {
        this.log = new RocksDBStore<>(logDir, Entry.class);
        this.logMetadata = new LogMetadata();
        persistLogMetadataToDiskShutdownHook(logMetadataDir);
    }

    private void persistLogMetadataToDiskShutdownHook(String logMetadataDir) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try (FileOutputStream fs = new FileOutputStream(logMetadataDir);
                 ObjectOutputStream os = new ObjectOutputStream(fs)) {
                os.writeObject(logMetadata);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    @Override
    public void appendEntry(int index, Entry entry) {
        try {
            log.put(Index.newBuilder().setIndex(index).build(), entry);
            logMetadata.setLastLogIndex(index);
            logMetadata.setLastLogTerm(entry.getTerm());
        } catch (Exception e) {
            throw new RuntimeException(e);      // TODO: handle exception in a better manner
        }
    }

    @Override
    public int getLastLogTerm() {
        return logMetadata.getLastLogTerm();
    }

    @Override
    public int getLastLogIndex() {
        return logMetadata.getLastLogIndex();
    }

    @Override
    public void deleteLastEntry() {

    }

    @Override
    public Entry getEntry(int index) {
        try {
            return log.get(Index.newBuilder().setIndex(index).build());
        } catch (Exception e) {
            throw new RuntimeException(e);      // TODO: handle exception in a better manner
        }
    }

    @Override
    public Entry getLastEntry() {
        try {
            return log.get(Index.newBuilder().setIndex(logMetadata.getLastLogIndex()).build());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getLastApplied() {
        return logMetadata.getLastApplied();
    }

    private static class LogMetadata implements Serializable {
        @Serial
        private static final long serialVersionUID = 18293719811127L;

        private int lastLogTerm;
        private int lastLogIndex;

        private int lastApplied;

        private LogMetadata() {
            this.lastLogTerm = 0;
            this.lastLogIndex = 0;
            this.lastApplied = 0;
        }

        public void setLastLogTerm(int lastLogTerm) {
            this.lastLogTerm = lastLogTerm;
        }

        public void setLastLogIndex(int lastLogIndex) {
            this.lastLogIndex = lastLogIndex;
        }

        public int getLastLogTerm() {
            return lastLogTerm;
        }

        public int getLastLogIndex() {
            return lastLogIndex;
        }

        public int getLastApplied() {
            return lastApplied;
        }

        public void setLastApplied(int lastApplied) {
            this.lastApplied = lastApplied;
        }
    }
}
