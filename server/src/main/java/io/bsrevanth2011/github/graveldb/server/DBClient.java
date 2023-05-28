package io.bsrevanth2011.github.graveldb.server;

import io.bsrevanth2011.github.graveldb.*;
import io.bsrevanth2011.github.graveldb.db.DB;
import io.grpc.stub.StreamObserver;

import java.rmi.ServerException;

public class DBClient extends DatabaseServiceGrpc.DatabaseServiceImplBase {

    private final DB<Key, Value, Result> db;

    public DBClient(DB<Key, Value, Result> db) {
        this.db = db;
    }

    @Override
    public void get(Key key, StreamObserver<Result> responseObserver) {
        if (!db.isMaster()) {
            completeExceptionally(responseObserver);
            return;
        }

        try {
            Result result = db.get(key);
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void put(KeyValuePair keyValuePair, StreamObserver<Result> responseObserver) {
        if (!db.isMaster()) {
            completeExceptionally(responseObserver);
            return;
        }

        try {
            Result result = db.put(keyValuePair.getKey(), keyValuePair.getValue());
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void delete(Key key, StreamObserver<Result> responseObserver) {
        if (!db.isMaster()) {
            completeExceptionally(responseObserver);
            return;
        }

        try {
            Result result = db.delete(key);
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    private void completeExceptionally(StreamObserver<?> responseObserver) {
        responseObserver.onError(new ServerException("Node is not master. " +
                "Client is responsible for redirecting request to master node"));
    }

}
