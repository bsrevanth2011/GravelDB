package io.bsrevanth2011.github.graveldb.server;

import io.bsrevanth2011.github.graveldb.*;
import io.grpc.stub.StreamObserver;

import java.util.function.Function;
import java.util.logging.Logger;

public class GravelDBService extends GravelDBGrpc.GravelDBImplBase {

    private static final Logger logger  = Logger.getLogger(GravelDBService.class.getName());

    private final Function<VoteRequest, VoteResponse> voteRequestHandler;

    public GravelDBService(Function<VoteRequest, VoteResponse> voteRequestHandler) {
        this.voteRequestHandler = voteRequestHandler;
    }

    @Override
    public void requestVote(VoteRequest voteRequest, StreamObserver<VoteResponse> streamObserver) {
        streamObserver.onNext(voteRequestHandler.apply(voteRequest));
        streamObserver.onCompleted();
    }

    @Override
    public void beatHeart(HeartBeat heartBeat, StreamObserver<Empty> streamObserver) {
        streamObserver.onNext(Empty.getDefaultInstance());
        logger.info("Received heart beat from " + heartBeat.getId());
        streamObserver.onCompleted();
    }
}
