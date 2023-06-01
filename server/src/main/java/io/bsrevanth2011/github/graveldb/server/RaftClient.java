package io.bsrevanth2011.github.graveldb.server;

import io.bsrevanth2011.github.graveldb.*;
import io.bsrevanth2011.github.graveldb.db.DB;
import io.grpc.stub.StreamObserver;

public class RaftClient extends DatabaseServiceGrpc.DatabaseServiceImplBase {

    private final DB<Key, Value> db;

    public RaftClient(DB<Key, Value> db) {
        this.db = db;
    }

    @Override
    public void get(Key key, StreamObserver<Result> responseObserver) {

        if (isNotLeader()) {
            completeObserverWithLeaderInfo(responseObserver);
        } else {
            try {
                Value value = db.get(key);
                Result.Status status = value == null ? Result.Status.DATA_NOT_FOUND : Result.Status.SUCCESS;
                Result.Builder resultBuilder = Result.newBuilder().setStatus(status);
                if (value != null) resultBuilder.setValue(value);
                responseObserver.onNext(resultBuilder.build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }
    }

    @Override
    public void put(KeyValuePair keyValuePair, StreamObserver<Result> responseObserver) {

        if (isNotLeader()) {
            completeObserverWithLeaderInfo(responseObserver);
        } else {
            try {
                db.put(keyValuePair.getKey(), keyValuePair.getValue());
                responseObserver.onNext(Result.newBuilder().setStatus(Result.Status.SUCCESS).build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }
    }

    @Override
    public void delete(Key key, StreamObserver<Result> responseObserver) {

        if (isNotLeader()) {
            completeObserverWithLeaderInfo(responseObserver);
        } else {
            try {
                db.delete(key);
                responseObserver.onNext(Result.newBuilder().setStatus(Result.Status.SUCCESS).build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }
    }

    private boolean isNotLeader() {
        return !db.isLeader();
    }

    private void completeObserverWithLeaderInfo(StreamObserver<Result> responseObserver) {
        LeaderInfo leaderInfo = db.getLeaderInfo();
        Result.Status status = leaderInfo.getLeaderId() == 0
                ? Result.Status.LEADER_NOT_ELECTED
                : Result.Status.NOT_AN_ELECTED_LEADER;
        responseObserver.onNext(Result.newBuilder().setLeaderInfo(leaderInfo).setStatus(status).build());
        responseObserver.onCompleted();
    }
}
