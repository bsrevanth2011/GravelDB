package io.bsrevanth2011.github.graveldb.server;

import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Message;
import io.bsrevanth2011.github.graveldb.*;
import io.bsrevanth2011.github.graveldb.log.Log;
import io.bsrevanth2011.github.graveldb.log.PersistentLog;
import io.bsrevanth2011.github.graveldb.util.ElectionTimer;
import io.bsrevanth2011.github.graveldb.util.HeartBeatTimer;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Maps;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.bsrevanth2011.github.graveldb.server.GravelDBServerUtil.*;

public class GravelDBConsensusService extends GravelDBConsensusServiceGrpc.GravelDBConsensusServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(GravelDBConsensusService.class.getName());
    private final Map<String, GravelDBConsensusServiceGrpc.GravelDBConsensusServiceStub> peerMap = Maps.mutable.of();
    private final ElectionTimer electionTimer = new ElectionTimer(requestVoteCallBack());
    private final HeartBeatTimer heartBeatTimer = new HeartBeatTimer(heartBeatCallback());
    private final State state;

    public GravelDBConsensusService(String instanceId,
                                    List<? extends Channel> channels,
                                    Map<String, String> conf) throws RocksDBException, IOException {

        this.state = new State(instanceId,
                new PersistentLog(conf.get("logDir"), conf.get("logMetadataDir")),
                channels,
                0,
                null,
                0,
                0,
                GravelDBConsensusService.ServerState.FOLLOWER);
        init();
    }

    private void init() {
        buildPeerMap(peerMap, state.getChannels());

        peerMap.keySet().forEach(instanceId -> {
            state.getNextIndex().put(instanceId, state.getCommitIndex().get() + 1);
            state.getMatchIndex().put(instanceId, 0);
        });

        electionTimer.start();
    }

    @Override
    public void requestInstanceId(Info requestInfo, StreamObserver<Info> streamObserver) {
        logger.info("Received RequestInstanceId RPC from node := " + requestInfo.getInstanceId());

        Info responseInfo = Info.newBuilder().setInstanceId(state.getInstanceId()).build();

        completeObserver(streamObserver, responseInfo);
    }

    @Override
    public void requestVote(VoteRequest voteRequest, StreamObserver<VoteResponse> streamObserver) {
        logger.info("Received RequestVote RPC from node := " + voteRequest.getCandidateId());

        VoteResponse voteResponse = VoteResponse.newBuilder()
                .setTerm(state.getTerm().get())
                .setVoteGranted(isVoteGranted(voteRequest,
                        state.getVotedFor(),
                        state.getTerm().get(),
                        state.getCommitIndex().get()))
                .build();

        if (voteResponse.getVoteGranted()) {
            state.setVotedFor(voteRequest.getCandidateId());
        }

        completeObserver(streamObserver, voteResponse);
    }

    @Override
    public void appendEntries(AppendEntriesRequest appendEntriesRequest,
                              StreamObserver<AppendEntriesResponse> streamObserver) {

        electionTimer.restart();

        boolean isMoreUpdateToDate = isMoreUpToDate(appendEntriesRequest);

        if (isMoreUpdateToDate && !state.getLeaderId().equals(appendEntriesRequest.getLeaderId())) {
            state.setLeaderId(appendEntriesRequest.getLeaderId());
        }

        if (!state.getServerState().equals(ServerState.FOLLOWER) && isMoreUpdateToDate) {
            stepDown();
        }

        boolean isLogConsistent = isLogConsistent(appendEntriesRequest);

        if (isLogConsistent) {
            int index = appendEntriesRequest.getPrevLogIndex();
            for (Entry newEntry : appendEntriesRequest.getEntriesList()) {
                state.getLog().appendEntry(++index, newEntry);
            }
        }

        logger.info("Received AppendEntries RPC from node := " + appendEntriesRequest.getLeaderId());

        AppendEntriesResponse appendEntriesResponse = AppendEntriesResponse.newBuilder()
                .setTerm(state.getTerm().get())
                .setSuccess(isMoreUpdateToDate && isLogConsistent)
                .build();

        completeObserver(streamObserver, appendEntriesResponse);
    }

    private boolean isLogConsistent(AppendEntriesRequest appendEntriesRequest) {
        return (appendEntriesRequest.getPrevLogIndex() == state.getCommitIndex().get());
    }

    @Override
    public void getMaster(Empty empty, StreamObserver<Info> streamObserver) {
        if (state.getLeaderId() == null) {
            completeObserverExceptionally(streamObserver, new IllegalStateException("Leader Id not set yet"));
            return;
        }

        completeObserver(streamObserver, Info.newBuilder().setInstanceId(state.getLeaderId()).build());
    }

    @Override
    public void getConsensus(Command command, StreamObserver<Consensus> streamObserver) {

        if (state.getLeaderId() == null) {
            completeObserverExceptionally(streamObserver, new IllegalStateException("Leader Id not set yet"));
            return;
        }

        if (!state.getInstanceId().equals(state.getLeaderId())) {
            peerMap.get(state.getLeaderId()).getConsensus(command,
                    unaryCallStreamObserver(consensus -> completeObserver(streamObserver, consensus)));
            return;
        }

        final int majority = calculateMajority(peerMap.size() + 1);
        AtomicInteger acceptCount = new AtomicInteger(1);
        AtomicInteger rejectCount = new AtomicInteger();
        SettableFuture<Consensus> consensusFuture = SettableFuture.create();

        if (acceptCount.incrementAndGet() >= majority) {
            consensusFuture.set(Consensus.newBuilder().setAchieved(true).build());
        }

        peerMap.forEach((instanceId, peer) -> {

                    AppendEntriesRequest request = AppendEntriesRequest
                            .newBuilder()
                            .setTerm(state.getTerm().get())
                            .setLeaderId(state.getInstanceId())
                            .setPrevLogTerm(state.getTerm().get())
                            .addAllEntries(addEntriesInRange(state.getMatchIndex().get(instanceId) + 1, state.getCommitIndex().get()))
                            .addEntries(Entry.newBuilder().setTerm(state.getTerm().get()).setCommand(command))
                            .setPrevLogIndex(state.getMatchIndex().get(instanceId))
                            .setLeaderCommit(state.getCommitIndex().get())
                            .build();

                    peer.appendEntries(request, unaryCallStreamObserver(appendEntriesResponse -> {

                        synchronized (consensusFuture) {

                            if (consensusFuture.isDone()) return;

                            if (appendEntriesResponse.getSuccess()) {
                                if (acceptCount.incrementAndGet() >= majority) {
                                    consensusFuture.set(Consensus.newBuilder().setAchieved(true).build());
                                }
                                state.getMatchIndex().put(instanceId, state.getCommitIndex().get());
                            } else {

                                state.getMatchIndex().put(instanceId, state.getMatchIndex().get(instanceId) - 1);

                                if (rejectCount.getAndIncrement() >= majority) {
                                    consensusFuture.set(Consensus.newBuilder().setAchieved(false).build());
                                }

                                if (appendEntriesResponse.getTerm() > state.getTerm().get()) {
                                    consensusFuture.set(Consensus.newBuilder().setAchieved(false).build());
                                    stepDown();
                                }
                            }
                        }
                    }));
                }
        );

        try {
            Consensus consensus = consensusFuture.get();
            if (consensus.getAchieved()) {
                // commit to log if consensus achieved
                state.getLog().appendEntry(state.getCommitIndex().incrementAndGet(), Entry.newBuilder().setTerm(state.getTerm().get()).setCommand(command).build());
            }
            completeObserver(streamObserver, consensus);
        } catch (ExecutionException | InterruptedException e) {
            completeObserverExceptionally(streamObserver, e);
        }
    }

    private Iterable<Entry> addEntriesInRange(int start, int end) {
        List<Entry> entries = Lists.mutable.of();
        while (start < end) {
            entries.add(state.getLog().getEntry(start++));
        }

        return entries;
    }

    private boolean isMoreUpToDate(AppendEntriesRequest appendEntriesRequest) {
        return state.getTerm().get() < appendEntriesRequest.getTerm()
                || (state.getTerm().get() == appendEntriesRequest.getTerm()
                && state.getCommitIndex().get() < appendEntriesRequest.getPrevLogIndex());
    }

    private Runnable heartBeatCallback() {
        return () -> {
            // send heartbeat messages to each peer
            peerMap.forEach((instanceId, peer) -> {

                int lastLogIndex = state.getMatchIndex().getOrDefault(instanceId, 0);

                peer.appendEntries(
                        AppendEntriesRequest
                                .newBuilder()
                                .setTerm(state.getTerm().get())
                                .setLeaderId(state.getInstanceId())
                                .setPrevLogIndex(lastLogIndex)
                                .setPrevLogTerm(getPrevLogTerm(lastLogIndex))
                                .addAllEntries(addEntriesInRange(lastLogIndex + 1, state.getNextIndex().get(instanceId)))
                                .build(),
                        unaryCallStreamObserver(appendEntriesResponse -> {
                            if (hasHigherTerm(appendEntriesResponse)) {     // instance has higher term than leader
                                stepDown();
                            } else {        // lastLogEntry did not match
                                state.getMatchIndex().put(instanceId, state.getMatchIndex().get(instanceId) - 1);
                            }
                        }));
            });
        };
    }

    private int getPrevLogTerm(Integer lastLogIndex) {
        if (lastLogIndex == 0) {
            return 0;
        } else if (lastLogIndex == state.getLog().getLastLogIndex()) {
            return state.getLog().getLastLogTerm();
        } else {
            return state.getLog().getEntry(lastLogIndex).getTerm();
        }
    }

    @SuppressWarnings("all")
    private Runnable requestVoteCallBack() {
        return () -> {
            switch (state.getServerState()) {
                case FOLLOWER, CANDIDATE -> {
                    contestForLeaderElection();
                }
                case LEADER -> {
                    // already leader, do nothing
                }
            }
        };
    }

    private void contestForLeaderElection() {

        state.setServerState(ServerState.CANDIDATE);
        state.getTerm().incrementAndGet();
        state.setVotedFor(state.getInstanceId());

        AtomicInteger numVotes = new AtomicInteger(1);      // self vote
        int majority = GravelDBServerUtil.calculateMajority(peerMap.size() + 1);        // simple majority

        if (numVotes.get() >= majority) {
            logger.info(String.format("Elected as leader after getting votes from %d nodes " +
                    "in the cluster", numVotes.get()));

            state.setServerState(ServerState.LEADER);
            state.setLeaderId(state.getInstanceId());
            electionTimer.cancel();
            heartBeatTimer.start();
        }

        peerMap.forEach((instanceId, peer) -> {
            VoteRequest voteRequest = VoteRequest.newBuilder()
                    .setTerm(state.getTerm().get())
                    .setCandidateId(state.getInstanceId())
                    .setLastLogIndex(state.getCommitIndex().get())
                    .setLastLogTerm(state.getTerm().get())
                    .build();
            peerMap.get(instanceId).requestVote(voteRequest, unaryCallStreamObserver(voteResponse -> {

                logger.info(String.format("Instance := %s %s vote",
                        instanceId, voteResponse.getVoteGranted() ? "GRANTED" : "DENIED"));

                // if the server is not a candidate, either it has received majority votes
                // and become leader or it has stepped down to be a follower again
                if (!state.getServerState().equals(ServerState.CANDIDATE)) return;

                if (voteResponse.getVoteGranted()
                        && numVotes.incrementAndGet() >= majority) {

                    logger.info(String.format("Elected as leader after getting votes from %d nodes " +
                            "in the cluster", numVotes.get()));

                    state.setServerState(ServerState.LEADER);
                    state.setLeaderId(state.getInstanceId());
                    electionTimer.cancel();
                    heartBeatTimer.start();

                } else if (!voteResponse.getVoteGranted()
                        && voteResponse.getTerm() > state.getTerm().get()) {

                    logger.info(String.format("Received response with higher term id := %d, " +
                            "reverting back to FOLLOWER STATE", voteResponse.getTerm()));

                    stepDown();
                }
            }));
        });
    }

    private boolean hasHigherTerm(AppendEntriesResponse appendEntriesResponse) {
        return !appendEntriesResponse.getSuccess()
                && appendEntriesResponse.getTerm() > state.getTerm().get();
    }

    @SuppressWarnings("all")
    private void buildPeerMap(
            Map<String, GravelDBConsensusServiceGrpc.GravelDBConsensusServiceStub> peerMap,
            List<? extends Channel> channels
    ) {

        channels.stream()
                .map(channel -> GravelDBConsensusServiceGrpc.newStub(channel))
                .forEach(peer -> peer.withWaitForReady().requestInstanceId(
                        Info.newBuilder().setInstanceId(state.getInstanceId()).build(),
                        unaryCallStreamObserver(info -> peerMap.put(info.getInstanceId(), peer))
                ));
    }

    private void stepDown() {
        switch (state.getServerState()) {

            case CANDIDATE -> {
                electionTimer.restart();
                state.setServerState(ServerState.FOLLOWER);
            }
            case LEADER -> {
                heartBeatTimer.cancel();
                electionTimer.restart();
                state.setServerState(ServerState.FOLLOWER);
            }
            case FOLLOWER -> {
                // nothing to do, already in the lowest possible state
            }
        }
    }

    private static class State {
        private final String instanceId;
        private final Log log;
        private final List<? extends Channel> channels;
        private final AtomicInteger term;
        private final AtomicInteger commitIndex;
        private String votedFor;
        private int lastApplied;
        private String leaderId;
        private final Map<String, Integer> nextIndex;
        private final Map<String, Integer> matchIndex;
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
            this.commitIndex = new AtomicInteger(commitIndex);
            this.lastApplied = lastApplied;
            this.serverState = serverState;
            this.nextIndex = new ConcurrentHashMap<>();
            this.matchIndex = new ConcurrentHashMap<>();
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

        public AtomicInteger getCommitIndex() {
            return commitIndex;
        }

        public void setCommitIndex(int commitIndex) {
            this.commitIndex.getAndSet(commitIndex);
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

        public Map<String, Integer> getNextIndex() {
            return nextIndex;
        }

        public Map<String, Integer> getMatchIndex() {
            return matchIndex;
        }

        public String getLeaderId() {
            return leaderId;
        }

        public void setLeaderId(String leaderId) {
            this.leaderId = leaderId;
        }
    }

    enum ServerState {
        FOLLOWER,
        CANDIDATE,
        LEADER;
    }
}
