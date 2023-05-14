package io.bsrevanth2011.github.graveldb.server;

import com.google.protobuf.Internal;
import io.bsrevanth2011.github.graveldb.Empty;
import io.bsrevanth2011.github.graveldb.GravelDBGrpc;
import io.bsrevanth2011.github.graveldb.HeartBeat;
import io.bsrevanth2011.github.graveldb.VoteResponse;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.impl.list.mutable.ListAdapter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class GravelDBServer {

    private final int port;
    private final Server server;

    private final int ELECTION_TIMEOUT = 5_000; // 5 seconds
    private Map<String, GravelDBGrpc.GravelDBBlockingStub> peerMap;

    private final ServerState serverState = ServerState.FOLLOWER;

    private enum ServerState {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    private static final Logger logger = Logger.getLogger(GravelDBService.class.getName());

    public GravelDBServer(int port) {
        this(ServerBuilder.forPort(port), port);
    }

    /**
     * Create a GravelDB server using serverBuilder as a base and features as data.
     */
    public GravelDBServer(ServerBuilder<?> serverBuilder, int port) {
        this.port = port;
        this.server = serverBuilder
                .addService(new GravelDBService((voteRequest) -> VoteResponse.getDefaultInstance()))
                .build();
    }

    public void start(List<String> targets) throws IOException, InterruptedException {
        server.start();
        logger.info("Server started, listening on " + port);

        registerTargets(targets);
        initiateHeartBeatSharing();
    }

    private void registerTargets(List<String> targets) {
        peerMap = ListAdapter.adapt(targets)
                .toMap(
                        target -> target,
                        target -> GravelDBGrpc
                                .newBlockingStub(ManagedChannelBuilder
                                        .forTarget(target)
                                        .usePlaintext()
                                        .build())
                                .withWaitForReady());
    }

    private void initiateHeartBeatSharing() {
        new Timer().schedule(new TimerTask() {
            @SuppressWarnings("ResultOfMethodCallIgnored")
            @Override
            public void run() {
                peerMap.forEach((target, peer) -> {
                    peer.beatHeart(HeartBeat.newBuilder().setId(port).build());
                    logger.info(String.format("Heart beat sent to peer %s successfully", target.split(":")[1]));
                });
            }
        }, 0, ELECTION_TIMEOUT);
    }


}