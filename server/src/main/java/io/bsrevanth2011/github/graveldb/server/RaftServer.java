package io.bsrevanth2011.github.graveldb.server;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.bsrevanth2011.github.graveldb.*;
import io.bsrevanth2011.github.graveldb.db.DB;
import io.bsrevanth2011.github.graveldb.log.Log;
import io.bsrevanth2011.github.graveldb.log.PersistentLog;
import io.bsrevanth2011.github.graveldb.util.CountdownTimer;
import io.grpc.stub.StreamObserver;
import lombok.Getter;
import lombok.Setter;
import org.eclipse.collections.api.factory.Lists;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static io.bsrevanth2011.github.graveldb.server.RaftServer.ServerState.FOLLOWER;

@Getter
@Setter
public class RaftServer extends ConsensusServerGrpc.ConsensusServerImplBase implements DB<Key, Value> {

    private static final Logger logger = LoggerFactory.getLogger(RaftServer.class);
    private final ReentrantLock lock = new ReentrantLock();

    private final int instanceId;
    private final Log log;
    private final StateMachine stateMachine;
    private final ServerStub[] remoteServers;
    private final CountdownTimer electionCountdown = new CountdownTimer();

    private int leaderId;
    private int currentTerm;
    private int commitIndex;
    private int votedFor;
    private int lastApplied;
    private ServerState serverState;

    public RaftServer(int instanceId,
                      ServerStubConfig[] stubConfigs) throws RocksDBException {

        this.instanceId = instanceId;
        this.serverState = FOLLOWER;
        this.log = new PersistentLog();
        this.currentTerm = log.getLastLogTerm();
        this.stateMachine = new StateMachine();
        this.remoteServers = Arrays.stream(stubConfigs)
                .map(stubConfig -> new ServerStub(stubConfig, log.getLastLogIndex() + 1))
                .toArray(ServerStub[]::new);
        restartElectionTimer();
    }

    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> responseObserver) {
        try {
            responseObserver.onNext(handleAppendEntries(request));
        } catch (Exception e) {
            responseObserver.onError(e);
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void requestVote(VoteRequest request, StreamObserver<VoteResponse> responseObserver) {
        try {
            responseObserver.onNext(handleRequestVote(request));
        } catch (Exception e) {
            responseObserver.onError(e);
        } finally {
            responseObserver.onCompleted();
        }
    }

    private AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {

        try {
            lock.lock();

            logger.info("Received AppendEntries request from server {}", request.getLeaderId());
            AppendEntriesResponse.Builder responseBuilder = AppendEntriesResponse.newBuilder();

            if (request.getTerm() < currentTerm) {
                logger.trace("Server {} is stale. currentTerm:= {}, caller's term := {}",
                        request.getLeaderId(), currentTerm, request.getLeaderId());
                return responseBuilder.setTerm(currentTerm).setSuccess(false).build();
            }

            if (currentTerm < request.getTerm()) {
                logger.info("Received AppendEntries request from server {} with higher term := {}, currentTerm := {}",
                        request.getLeaderId(), request.getTerm(), currentTerm);
                responseBuilder.setTerm(request.getTerm());
            }

            stepDown(request.getTerm());

            if (leaderId == 0) {
                leaderId = request.getLeaderId();
                logger.info("Server {} elected as new leader for term {}", leaderId, currentTerm);
            } else {
                assert leaderId == request.getLeaderId();
            }

            for (int index = request.getPrevLogIndex() + 1; index <= commitIndex; index++) {
                log.deleteEntry(index);
            }

            if (request.getPrevLogIndex() > log.getLastLogIndex()) {
                logger.info("Rejecting AppendEntries request from server {}. Reason: Missing Entries. Previous log index" +
                                " in the request is {}, and last log index in the log is {}",
                        request.getLeaderId(), request.getPrevLogIndex(), log.getLastLogIndex());
                return responseBuilder.setSuccess(false).build();
            }

            int prevLogTerm = request.getPrevLogIndex() <= 0 ? 0 : log.getEntry(request.getPrevLogIndex()).getTerm();
            if (prevLogTerm != request.getPrevLogTerm()) {
                logger.info("Rejecting AppendEntries request from server {}. Reason: Previous " +
                        "log entries do not match", request.getLeaderId());
                return responseBuilder.setSuccess(false).build();
            }

            responseBuilder.setSuccess(true);

            int index = request.getPrevLogIndex();
            for (Entry entry : request.getEntriesList()) {

                ++index;

                if (log.getLastLogIndex() >= index) {
                    if (log.getEntry(index).getTerm() == entry.getTerm()) {
                        continue; // no need to overwrite the same entry
                    }
                    logger.info("Purging entries from {} to {} (both inclusive)",
                            index, log.getLastLogIndex());

                    for (int delIndex = index;
                         delIndex <= log.getLastLogIndex();
                         delIndex++) {
                        log.deleteEntry(delIndex);
                    }
                }

                log.appendEntry(index, entry);
                logger.info("Added entry in the log for index {} and term {}", log.getLastLogIndex(), entry.getTerm());
            }

            if (request.getLeaderCommit() > getCommitIndex()) {
                logger.trace("Leader commit {} is greater than commit index {}. " +
                                "Updated commit index to min(leaderCommit, index of last new entry)"
                        , request.getLeaderCommit(), getCommitIndex());
                setCommitIndex(Math.min(index, request.getLeaderCommit()));
            }

            //  to ensure the processing delay doesn't impact the election timeout
            restartElectionTimer();
            logger.info("Accepting AppendEntries request from server {} for term {}",
                    request.getLeaderId(), getCurrentTerm());

            return responseBuilder.build();
        } finally {
            lock.unlock();
        }
    }

    private VoteResponse handleRequestVote(VoteRequest request) {

        try {
            lock.lock();

            VoteResponse.Builder responseBuilder = VoteResponse.newBuilder();

            boolean isLogAhead = (request.getLastLogTerm() > log.getLastLogTerm()
                    || (request.getLastLogTerm() == log.getLastLogTerm()
                    && request.getLastLogIndex() >= log.getLastLogIndex()));

            if (request.getTerm() > currentTerm) {
                logger.info("Received RequestVote from server {} with term {} which is" +
                        " higher than currentTerm {}", request.getCandidateId(), request.getTerm(), currentTerm);
                stepDown(request.getTerm());
            }

            if (request.getTerm() == currentTerm) {
                if (!isLogAhead) {
                    logger.info("Rejecting RequestVote for term {} from server {}," +
                                    " since current log is ahead of server's log.",
                            currentTerm, request.getCandidateId());
                    return responseBuilder.setTerm(currentTerm).setVoteGranted(false).build();
                }

                if (votedFor != 0) {
                    logger.info("Rejecting RequestVote for term {} from server {}," +
                            " since either vote for already casted vote to or heard from" +
                            " server {} in the current term.", currentTerm, request.getCandidateId(), leaderId);
                    return responseBuilder.setTerm(currentTerm).setVoteGranted(false).build();
                }

                stepDown(currentTerm);
                setVotedFor(request.getCandidateId());
            }

            return responseBuilder.setTerm(currentTerm).setVoteGranted(true).build();
        } finally {
            lock.unlock();
        }
    }

    private void restartElectionTimer() {
        electionCountdown.startCountdown(this::contestForElections,
                GravelDBServerConfiguration.ELECTION_TIMEOUT_MILLIS + generateRandomDelay());
    }

    private void startHeartBeatTimer() {
        electionCountdown.schedule(this::sendHeartBeat, GravelDBServerConfiguration.HEARTBEAT_INTERVAL_MILLIS);
    }

    private void contestForElections() {

        try {
            lock.lock();
            if (getServerState() != RaftServer.ServerState.LEADER) {

                setLeaderId(0);
                setCurrentTerm(getCurrentTerm() + 1);
                setVotedFor(getInstanceId());

                logger.info("Contesting for elections in term {}", getCurrentTerm());

                if (getServerState() != ServerState.CANDIDATE) {
                    setServerState(RaftServer.ServerState.CANDIDATE);
                }

                VoteRequest request = VoteRequest.newBuilder()
                        .setTerm(getCurrentTerm())
                        .setCandidateId(getInstanceId())
                        .setLastLogTerm(getLog().getLastLogTerm())
                        .setLastLogIndex(getLog().getLastLogIndex())
                        .build();

                final int majority = (remoteServers.length + 1) / 2 + 1;
                AtomicInteger
                        accepts = new AtomicInteger(1),
                        rejects = new AtomicInteger(0);
                for (ServerStub stub : remoteServers) {

                    stub.requestVote(request, new StreamObserver<>() {
                        @Override
                        public void onNext(VoteResponse response) {

                            try {
                                lock.lock();
                                if (getServerState() != RaftServer.ServerState.CANDIDATE) {
                                    logger.info("Received response from {} for RequestVote call in term {}. " +
                                                    "But server not in candidate state anymore, therefore ignoring response.",
                                            stub.getInstanceId(), getCurrentTerm());
                                    return;
                                }

                                if (response.getVoteGranted()) {
                                    logger.info("Vote granted by server {} for election term {}",
                                            stub.getInstanceId(), request.getTerm());
                                    accepts.incrementAndGet();
                                    if (accepts.get() >= majority) {
                                        becomeLeader();
                                    }
                                } else {
                                    logger.info("Vote denied by server {} for election term {}. Returned term {} in response",
                                            stub.getInstanceId(), request.getTerm(), response.getTerm());

                                    if (response.getTerm() > getCurrentTerm()) {
                                        logger.info("Received a response for AppendEntries call from server {} with " +
                                                        " term {} which is higher than current term {}. Initiating step down procedure...",
                                                stub.getInstanceId(), response.getTerm(), getCurrentTerm());
                                        stepDown(response.getTerm());
                                        return;
                                    }

                                    if (rejects.incrementAndGet() >= majority) {
                                        logger.info("Received rejection for VoteRequest call from {} servers which is more than or equal to " +
                                                "the minimum required value of {} out of {}", rejects.get(), majority, remoteServers.length + 1);
                                        stepDown(getCurrentTerm());
                                    }
                                }
                            } finally {
                                lock.unlock();
                            }
                        }

                        @Override
                        public void onError(Throwable t) {

                            try {
                                lock.lock();
                                if (getServerState() != RaftServer.ServerState.CANDIDATE) {
                                    logger.info("Received response from {} for RequestVote call in term {}. " +
                                                    "But server not in candidate state anymore, therefore ignoring response.",
                                            stub.getInstanceId(), getCurrentTerm());
                                    return;
                                }

                                logger.error("Error occurred during VoteRequest call to server {} := {}", stub.getInstanceId(), t.getMessage());

                                if (getServerState() != RaftServer.ServerState.CANDIDATE) {
                                    return;
                                }

                                if (rejects.incrementAndGet() >= majority) {
                                    logger.info("Received rejection for VoteRequest call from {} servers which is more than or equal to " +
                                            "the minimum required value of {} out of {}", rejects.get(), majority, remoteServers.length + 1);
                                    stepDown(getCurrentTerm());
                                }
                            } finally {
                                lock.unlock();
                            }
                        }

                        @Override
                        public void onCompleted() {

                        }
                    });
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    private void sendHeartBeat() {
        logger.info("Sending heartbeat to all servers to convey leader status");
        try {
            replicateLog(Entry
                    .newBuilder()
                    .setTerm(getCurrentTerm())
                    .setCommand(Command
                            .newBuilder()
                            .setOp(Command.Op.NOOP).build())
                    .build())
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error occurred := " + e);
        }
    }

    private void becomeLeader() {

        try {
            lock.lock();
            logger.info("Elected as leader in term {} after receiving majority votes", getCurrentTerm());
            assert getServerState() == ServerState.CANDIDATE;

            setServerState(ServerState.LEADER);
            setLeaderId(getInstanceId());
            setVotedFor(0);

            startHeartBeatTimer();
        } finally {
            lock.unlock();
        }
    }

    private static long generateRandomDelay() {
        // generate a random delay between 0 and T
        return new Random().nextInt(0, GravelDBServerConfiguration.ELECTION_TIMEOUT_MILLIS / 1000) * 1000L;
    }

    private void stepDown(int newTerm) {
        assert currentTerm <= newTerm;

        if (currentTerm < newTerm) {
            logger.trace("Stepping down. Updating term from {} to {}", currentTerm, newTerm);
            setCurrentTerm(newTerm);
            setLeaderId(0);
            setVotedFor(0);
            setServerState(FOLLOWER);
        } else if (getServerState() != FOLLOWER) {
            setServerState(FOLLOWER);
        }

        restartElectionTimer();
    }

    @Override
    public Value get(Key key) {
        return stateMachine.get(key);
    }

    @Override
    public void put(Key key, Value value) {
        Entry entry = createPutEntry(key, value);
        try {
            boolean logReplicated = replicateLog(entry).get();
            if (logReplicated) {
                stateMachine.put(key, value);
                incrementCommitIndex();
            } else {
                throw new ReplicationFailureException();
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(Key key) {
        Entry entry = createDeleteEntry(key);

        try {
            boolean logReplicated = replicateLog(entry).get();
            if (logReplicated) {
                stateMachine.delete(key);
                incrementCommitIndex();
            } else {
                throw new ReplicationFailureException();
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void incrementCommitIndex() {
        setCommitIndex(getCommitIndex() + 1);
    }

    @CanIgnoreReturnValue
    private Future<Boolean> replicateLog(Entry... entries) {

        SettableFuture<Boolean> logReplicationFuture = SettableFuture.create();

        try {
            lock.lock();

            for (Entry entry : entries) {
                log.appendEntry(log.getLastLogIndex() + 1, entry);
            }

            AppendEntriesRequest.Builder requestBuilder = AppendEntriesRequest.newBuilder()
                    .setLeaderCommit(commitIndex)
                    .setLeaderId(getInstanceId());

            AtomicInteger accepts = new AtomicInteger(1);
            AtomicInteger rejects = new AtomicInteger(0);

            int majority = (remoteServers.length + 1) / 2 + 1;

            for (ServerStub stub : remoteServers) {

                int prevLogIndex = stub.getNextIndex() - 1;
                int prevLogTerm = prevLogIndex <= 0 ? 0 : log.getEntry(prevLogIndex).getTerm();

                AppendEntriesRequest request = requestBuilder.setPrevLogIndex(prevLogIndex)
                        .setPrevLogTerm(prevLogTerm)
                        .addAllEntries(log.getEntriesInRange(prevLogIndex + 1, log.getLastLogIndex()))        // pending entries
                        .addAllEntries(Lists.mutable.of(entries))       // new entries
                        .setTerm(getCurrentTerm())
                        .build();

                logger.info("Sending AppendEntries request to server {} in term {}", stub.getInstanceId(), getCurrentTerm());

                stub.appendEntries(request, new StreamObserver<>() {
                    @Override
                    public void onNext(AppendEntriesResponse response) {

                        try {
                            lock.lock();

                            if (getServerState() != ServerState.LEADER) {
                                logger.info("Received response from {} for AppendEntries call in term {}. " +
                                                "But server not in leader state anymore, therefore ignoring response.",
                                        stub.getInstanceId(), getCurrentTerm());
                                return;
                            }

                            if (response.getSuccess()) {

                                logger.info("Received acceptance for AppendEntries call from server {} in term {}",
                                        stub.getInstanceId(), getCurrentTerm());
                                accepts.getAndIncrement();

                                if (stub.getMatchIndex() < prevLogIndex + entries.length) {
                                    stub.setMatchIndex(prevLogIndex + entries.length);
                                    stub.setNextIndex(stub.getMatchIndex() + 1);
                                }

                                if (accepts.get() >= majority) {
                                    logReplicationFuture.set(true);
                                }
                            } else {

                                logger.info("Received rejection for AppendEntries call from server {} in term {}. " +
                                        "Decrementing the next index for the server by 1", stub.getInstanceId(), getCurrentTerm());
                                rejects.getAndIncrement();

                                stub.setNextIndex(stub.getNextIndex() - 1);

                                if (response.getTerm() > getCurrentTerm()) {
                                    logger.info("Received a response for AppendEntries call from server {} with " +
                                                    " term {} which is higher than current term {}. Initiating step down procedure...",
                                            stub.getInstanceId(), response.getTerm(), getCurrentTerm());
                                    stepDown(response.getTerm());
                                    return;
                                }

                                if (rejects.get() >= majority) {
                                    logger.info("Received rejection for AppendEntries call from {} servers which is less than " +
                                            "the minimum required value of {} out of {}", rejects.get(), majority, remoteServers.length + 1);
                                    stepDown(getCurrentTerm());
                                    logReplicationFuture.set(false);
                                }
                            }
                        } finally {
                            lock.unlock();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        logger.error("Error occurred during AppendEntries call to server" + stub.getInstanceId(), t);

                        try {
                            lock.lock();
                            if (rejects.incrementAndGet() >= majority) {
                                logger.info("Received rejection for AppendEntries call from {} servers which is less than " +
                                        "the minimum required value of {} out of {}", rejects.get(), majority, remoteServers.length + 1);
                                stepDown(getCurrentTerm());
                                logReplicationFuture.set(false);
                            }
                        } finally {
                            lock.unlock();
                        }
                    }

                    @Override
                    public void onCompleted() {
                    }
                });
            }

            return logReplicationFuture;
        } catch (Exception e) {
            logger.error("Exception occurred := " + e.getMessage());
            return Futures.immediateFailedFuture(e);
        } finally {
            lock.unlock();
        }
    }

    private Entry createDeleteEntry(Key key) {
        return Entry.newBuilder()
                .setTerm(getCurrentTerm())
                .setCommand(Command
                        .newBuilder()
                        .setOp(Command.Op.DELETE)
                        .setData(Data
                                .newBuilder()
                                .setKey(key)
                                .build()))
                .build();
    }

    private Entry createPutEntry(Key key, Value value) {
        return Entry.newBuilder()
                .setTerm(getCurrentTerm())
                .setCommand(Command
                        .newBuilder()
                        .setOp(Command.Op.PUT)
                        .setData(Data
                                .newBuilder()
                                .setKey(key)
                                .setValue(value)
                                .build()))
                .build();
    }

    enum ServerState {
        FOLLOWER,
        CANDIDATE,
        LEADER;
    }
}
