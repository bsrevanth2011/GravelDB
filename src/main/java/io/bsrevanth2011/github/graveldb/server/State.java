package io.bsrevanth2011.github.graveldb.server;

import io.grpc.Channel;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class State {
        private final String instanceId;
        private final Log log;
        private final List<? extends Channel> channels;
        private final AtomicInteger term;
        private String votedFor;
        private int commitIndex;
        private int lastApplied;
        private GravelDBConsensusService.ServerState serverState;

    public State(String instanceId,
                 Log log,
                 List<? extends Channel> channels,
                 int term,
                 String votedFor,
                 int commitIndex,
                 int lastApplied,
                 GravelDBConsensusService.ServerState serverState) {
        this.instanceId = instanceId;
        this.log = log;
        this.channels = channels;
        this.term = new AtomicInteger(term);
        this.votedFor = votedFor;
        this.commitIndex = commitIndex;
        this.lastApplied = lastApplied;
        this.serverState = serverState;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public Log getLog() {
        return log;
    }

    public List<? extends Channel> getChannels() {
        return channels;
    }

    public AtomicInteger getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term.getAndSet(term);
    }

    public String getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(String votedFor) {
        this.votedFor = votedFor;
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(int commitIndex) {
        this.commitIndex = commitIndex;
    }

    public int getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(int lastApplied) {
        this.lastApplied = lastApplied;
    }

    public GravelDBConsensusService.ServerState getServerState() {
        return serverState;
    }

    public void setServerState(GravelDBConsensusService.ServerState serverState) {
        this.serverState = serverState;
    }
}
