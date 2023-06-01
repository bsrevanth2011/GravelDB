package io.bsrevanth2011.github.graveldb.client;

import com.google.protobuf.ByteString;
import io.bsrevanth2011.github.graveldb.*;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.eclipse.collections.api.factory.Maps;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

public class SimpleBlockingDBClient {

    private final Map<Integer, DatabaseServiceGrpc.DatabaseServiceBlockingStub> stubMap = Maps.mutable.of();
    private int serverCount;
    private DatabaseServiceGrpc.DatabaseServiceBlockingStub stub;

    public SimpleBlockingDBClient() {
        init();
    }

    private void init() {

        YAMLConfiguration config = new YAMLConfiguration();
        try {
            config.read(SimpleBlockingDBClient.class.getClassLoader().getResourceAsStream("application.yml"));
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }

        this.serverCount = config.getInt("server.count");
        List<HierarchicalConfiguration<ImmutableNode>> servers = config.configurationsAt("server.stubs");

        for (var server : servers) {
            int instanceId = server.getInt("instanceId");
            String target = server.getString("target");
            stubMap.put(instanceId, buildStub(target));
        }

        this.stub = getStubAtRandom();
    }

    public String get(String key) {
        Result result = stub.get(createKey(key));
        if (result.getStatus().equals(Result.Status.SUCCESS)) {
            return resultToNullableString(result.getValue());
        } else {
            if (result.getStatus().equals(Result.Status.NOT_AN_ELECTED_LEADER)) {
                updateLeader(result);
                return get(key);
            } else if (result.getStatus().equals(Result.Status.DATA_NOT_FOUND)) {
                return null;
            } else if (result.getStatus().equals(Result.Status.LEADER_NOT_ELECTED)) {
                throw new RuntimeException("Leader not yet elected");
            } else {
                throw new RuntimeException("Unknown error");
            }
        }
    }

    public void put(String key, String value) {
        Result result = stub.put(createKeyValuePair(key, value));
        if (!result.getStatus().equals(Result.Status.SUCCESS)) {
            if (result.getStatus().equals(Result.Status.NOT_AN_ELECTED_LEADER)) {
                updateLeader(result);
                put(key, value);
            } else if (result.getStatus().equals(Result.Status.LEADER_NOT_ELECTED)) {
                throw new RuntimeException("Leader not yet elected");
            } else {
                throw new RuntimeException("Unknown error");
            }
        }
    }

    public void delete(String key) {
        Result result = stub.delete(createKey(key));
        if (!result.getStatus().equals(Result.Status.SUCCESS)) {
            if (result.getStatus().equals(Result.Status.NOT_AN_ELECTED_LEADER)) {
                updateLeader(result);
                delete(key);
            } else if (result.getStatus().equals(Result.Status.LEADER_NOT_ELECTED)) {
                throw new RuntimeException("Leader not yet elected");
            } else {
                throw new RuntimeException("Unknown error");
            }
        }
    }

    private void updateLeader(Result result) {
        if (result.hasLeaderInfo()) {
            LeaderInfo leaderInfo = result.getLeaderInfo();
            this.stub = stubMap.get(leaderInfo.getLeaderId());
        } else {
            throw new RuntimeException("Unknown error");
        }
    }

    private static String resultToNullableString(Value value) {
        return new String(value.getValue().toByteArray());
    }

    private Key createKey(String key) {
        Objects.requireNonNull(key);
        return Key.newBuilder().setKey(createByteString(key)).build();
    }

    private Value createValue(String value) {
        Objects.requireNonNull(value);
        return Value.newBuilder().setValue(createByteString(value)).build();
    }

    private KeyValuePair createKeyValuePair(String key, String value) {
        return KeyValuePair
                .newBuilder()
                .setKey(createKey(key))
                .setValue(createValue(value))
                .build();
    }

    private static ByteString createByteString(String value) {
        return ByteString.copyFrom(value.getBytes(Charset.defaultCharset()));
    }

    private static DatabaseServiceGrpc.DatabaseServiceBlockingStub buildStub(String target) {
        return DatabaseServiceGrpc
                .newBlockingStub(ManagedChannelBuilder
                        .forTarget(target)
                        .usePlaintext()
                        .build())
                .withWaitForReady();
    }

    private DatabaseServiceGrpc.DatabaseServiceBlockingStub getStubAtRandom() {
        return stubMap.get((new Random()).nextInt(1, serverCount + 1));
    }
}
