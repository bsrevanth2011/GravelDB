package io.bsrevanth2011.github.graveldb.server;

import io.bsrevanth2011.github.graveldb.Info;
import io.bsrevanth2011.github.graveldb.VoteRequest;
import io.bsrevanth2011.github.graveldb.VoteResponse;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Consumer;

public class GravelDBServerUtil {

    private static final Logger logger = LoggerFactory.getLogger(GravelDBServerUtil.class);

    static int calculateMajority(int numNodes) {

        // a simple majority
        return numNodes / 2 + 1;
    }

    static boolean isVoteGranted(VoteRequest voteRequest, String votedFor, int term, int commitIndex) {

        // if local log is more up-to-date than candidate's log, deny vote, else grant vote
        boolean isVoteGranted = ((votedFor == null
                || Objects.equals(votedFor, voteRequest.getCandidateId()))
                && voteRequest.getTerm() >= term
                && voteRequest.getLastLogIndex() >= commitIndex)
                || (!Objects.equals(votedFor, voteRequest.getCandidateId())
                && voteRequest.getTerm() > term);

        if (isVoteGranted) {
            logger.info("Granted vote request as candidate's log is at least as up-to-date as local log");
        } else {
            logger.info("Denied vote request as local log is more up-to-date than the candidate's log");
        }

        return isVoteGranted;
    }

    static <T> void completeObserver(StreamObserver<T> streamObserver, T result) {
        streamObserver.onNext(result);
        streamObserver.onCompleted();
    }

    static <T> void completeObserverExceptionally(StreamObserver<T> streamObserver, Throwable t) {
        streamObserver.onError(t);
        streamObserver.onCompleted();
    }


    static <T> StreamObserver<T> unaryCallStreamObserver(Consumer<T> onNextHandler) {
        return new StreamObserver<>() {
            @Override
            public void onNext(T value) {
                onNextHandler.accept(value);
            }

            @Override
            public void onError(Throwable t) {
                logger.error(String.format("Exception occurred | Error Message := %s %s Cause := %s",
                        t.getMessage(), System.lineSeparator(), t.getCause()));
            }

            @Override
            public void onCompleted() {
                // call to onNext implies completion since it is a unary response
            }
        };
    }
}
