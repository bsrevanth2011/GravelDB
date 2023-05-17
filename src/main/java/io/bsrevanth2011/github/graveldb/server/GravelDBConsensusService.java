package io.bsrevanth2011.github.graveldb.server;

import io.bsrevanth2011.github.graveldb.*;
import io.bsrevanth2011.github.graveldb.server.util.ElectionTimer;
import io.bsrevanth2011.github.graveldb.server.util.HeartBeatTimer;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import org.eclipse.collections.api.factory.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class GravelDBConsensusService extends GravelDBConsensusServiceGrpc.GravelDBConsensusServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(GravelDBConsensusService.class.getName());
    private final Map<String, GravelDBConsensusServiceGrpc.GravelDBConsensusServiceStub> peerMap = Maps.mutable.of();
    private final ElectionTimer electionTimer = ElectionTimer.createStarted(requestVoteCallBack());
    private final HeartBeatTimer heartBeatTimer = new HeartBeatTimer(heartBeatCallback());
    private final State state;

    public GravelDBConsensusService(State state) {
        this.state = state;
        init();
    }

    private void init() {
        buildPeerMap(peerMap, state.getChannels());
    }

    private Runnable heartBeatCallback() {
        return () -> {

            // send heartbeat messages to each peer
            peerMap.forEach((instanceId, peer) -> peer.appendEntries(
                    AppendEntriesRequest
                            .newBuilder()
                            .setTerm(state.getTerm().get())
                            .setLeaderId(state.getInstanceId())
                            .setPrevLogIndex(state.getCommitIndex())
                            .setPrevLogTerm(state.getTerm().get())
                            .build(),
                    new StreamObserver<>() {
                        @Override
                        public void onNext(AppendEntriesResponse appendEntriesResponse) {
                            if (!appendEntriesResponse.getSuccess()
                                    && appendEntriesResponse.getTerm() > state.getTerm().get()) {

                                stepDown();
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            logger.info("Error occurred while sending heartbeat to instance := " + instanceId);
                        }

                        @Override
                        public void onCompleted() {
                            // nothing to do, as leader doesn't wait for response from peers
                        }
                    }));
        };
    }

    @SuppressWarnings("all")
    private Runnable requestVoteCallBack() {
        return () -> {
            switch (state.getServerState()) {
                case FOLLOWER -> {
                    contestForLeaderElection();
                    break;
                }
                case CANDIDATE -> {
                    contestForLeaderElection();
                    break;
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
        AtomicInteger numVotes = new AtomicInteger(1);      // self vote
        int majority = GravelDBServerUtil.calculateMajority(peerMap.size() + 1);        // simple majority

        peerMap.forEach((instanceId, peer) -> {
            VoteRequest voteRequest = VoteRequest.newBuilder()
                    .setTerm(state.getTerm().get())
                    .setCandidateId(state.getInstanceId())
                    .setLastLogIndex(state.getCommitIndex())
                    .setLastLogTerm(state.getTerm().get())
                    .build();
            peerMap.get(instanceId).requestVote(voteRequest, new StreamObserver<>() {
                @Override
                public void onNext(VoteResponse voteResponse) {
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
                        electionTimer.cancel();
                        heartBeatTimer.start();

                    } else if (!voteResponse.getVoteGranted()
                            && voteResponse.getTerm() > state.getTerm().get()) {

                        logger.info(String.format("Received response with higher term id := %d, " +
                                "reverting back to FOLLOWER STATE", voteResponse.getTerm()));

                        stepDown();
                    }
                }

                @Override
                public void onError(Throwable t) {
                    logger.info("Error occurred while request vote from instance := " + instanceId);
                }

                @Override
                public void onCompleted() {
                }
            });
        });
    }

    @Override
    synchronized public void requestVote(VoteRequest voteRequest, StreamObserver<VoteResponse> streamObserver) {
        logger.info("Received RequestVote RPC from node := " + voteRequest.getCandidateId());

        VoteResponse voteResponse = VoteResponse.newBuilder()
                .setTerm(state.getTerm().get())
                .setVoteGranted(isVoteGranted(voteRequest))
                .build();

        if (voteResponse.getVoteGranted()) {
            state.setVotedFor(voteRequest.getCandidateId());
        }

        streamObserver.onNext(voteResponse);
        streamObserver.onCompleted();
    }

    @Override
    synchronized public void appendEntries(AppendEntriesRequest appendEntriesRequest,
                              StreamObserver<AppendEntriesResponse> streamObserver) {

        electionTimer.restart();

        boolean isAhead = (state.getTerm().get() < appendEntriesRequest.getTerm() ||
                (state.getTerm().get() == appendEntriesRequest.getTerm() && state.getCommitIndex() < appendEntriesRequest.getPrevLogIndex()));

        if (!state.getServerState().equals(ServerState.FOLLOWER) && isAhead) {
            stepDown();
        }

        logger.info("Received AppendEntries RPC from node := " + appendEntriesRequest.getLeaderId());

        AppendEntriesResponse appendEntriesResponse = AppendEntriesResponse.newBuilder()
                .setTerm(state.getTerm().get())
                .setSuccess(isAhead)
                .build();

        streamObserver.onNext(appendEntriesResponse);
        streamObserver.onCompleted();
    }

    @Override
    public void requestInstanceId(Info requestInfo, StreamObserver<Info> streamObserver) {
        logger.info("Received RequestInstanceId RPC from node := " + requestInfo.getInstanceId());

        Info responseInfo = Info.newBuilder().setInstanceId(state.getInstanceId()).build();

        streamObserver.onNext(responseInfo);
        streamObserver.onCompleted();
    }

    private boolean isVoteGranted(VoteRequest voteRequest) {

        // if local log is more up-to-date than candidate's log, deny vote, else grant vote
        boolean isVoteGranted = ((state.getVotedFor() == null
                || Objects.equals(state.getVotedFor(), voteRequest.getCandidateId()))
                && voteRequest.getTerm() >= state.getTerm().get()
                && voteRequest.getLastLogIndex() >= state.getCommitIndex())
                || (!Objects.equals(state.getVotedFor(), voteRequest.getCandidateId())
                && voteRequest.getTerm() > state.getTerm().get());

        if (isVoteGranted) {
            logger.info("Granted vote request as candidate's log is at least as up-to-date as local log");
        } else {
            logger.info("Denied vote request as local log is more up-to-date than the candidate's log");
        }

        return isVoteGranted;
    }

    @SuppressWarnings("all")
    private void buildPeerMap(
            Map<String, GravelDBConsensusServiceGrpc.GravelDBConsensusServiceStub> peerMap,
            List<? extends Channel> channels
    ) {

        channels.stream()
                .map(channel -> GravelDBConsensusServiceGrpc.newStub(channel))
                .forEach(peer -> peer.withWaitForReady().requestInstanceId(
                        Info
                                .newBuilder()
                                .setInstanceId(state.getInstanceId())
                                .build(),
                        new StreamObserver<>() {
                            @Override
                            public void onNext(Info info) {
                                logger.info(String.format("Instance id for host := %s is := %s",
                                        peer.getChannel().authority(), info.getInstanceId()));
                                peerMap.put(info.getInstanceId(), peer);
                            }

                            @Override
                            public void onError(Throwable t) {
                                logger.info("Following error occurred while fetching instance" +
                                        " id from server := " + peer.getChannel().authority());
                            }

                            @Override
                            public void onCompleted() {
                            }
                        }));
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

    enum ServerState {
        FOLLOWER,
        CANDIDATE,
        LEADER;
    }
}
