package io.bsrevanth2011.github.graveldb.server;


import io.bsrevanth2011.github.graveldb.*;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

public class ServerStub {

    private final int instanceId;
    private int nextIndex;
    private int matchIndex;
    private final ConsensusServerGrpc.ConsensusServerStub stub;

    public ServerStub(ServerStubConfig stubConfig, int nextIndex) {
        this.instanceId = stubConfig.instanceId();
        this.nextIndex = nextIndex;
        this.stub = ConsensusServerGrpc.newStub(ManagedChannelBuilder.forTarget(stubConfig.target()).usePlaintext().build());
    }

    public int getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(int nextIndex) {
        this.nextIndex = nextIndex;
    }

    public int getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(int matchIndex) {
        this.matchIndex = matchIndex;
    }

    public int getInstanceId() {
        return instanceId;
    }

    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> responseObserver) {
        stub.appendEntries(request, responseObserver);
    }

    public void requestVote(VoteRequest request, StreamObserver<VoteResponse> responseObserver) {
        stub.requestVote(request, responseObserver);
    }
}
