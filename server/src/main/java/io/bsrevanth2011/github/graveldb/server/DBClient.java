package io.bsrevanth2011.github.graveldb.server;

import io.bsrevanth2011.github.graveldb.*;
import io.bsrevanth2011.github.graveldb.db.DB;
import io.bsrevanth2011.github.graveldb.util.ContextAwareThreadPoolExecutor;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DBClient extends DatabaseServiceGrpc.DatabaseServiceImplBase {

    private static final int THREADPOOL_SIZE = 100;

    private final DB<Key, Value> db;
    private final ExecutorService executor = ContextAwareThreadPoolExecutor
            .newWithContext(THREADPOOL_SIZE, THREADPOOL_SIZE, Integer.MAX_VALUE, TimeUnit.MILLISECONDS);

    public DBClient(DB<Key, Value> db) {
        this.db = db;
    }

    @Override
    public void get(Key key, StreamObserver<Result> responseObserver) {
        try {
            Value value = db.get(key);
            responseObserver.onNext(Result.newBuilder().setValue(value).setStatus(true).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void put(KeyValuePair keyValuePair, StreamObserver<Result> responseObserver) {
        try {
            db.put(keyValuePair.getKey(), keyValuePair.getValue());
            responseObserver.onNext(Result.newBuilder().setStatus(true).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void delete(Key key, StreamObserver<Result> responseObserver) {
        try {
            db.delete(key);
            responseObserver.onNext(Result.newBuilder().setStatus(true).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

}
