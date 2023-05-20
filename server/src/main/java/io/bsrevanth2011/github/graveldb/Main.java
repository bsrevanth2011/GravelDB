package io.bsrevanth2011.github.graveldb;

import io.bsrevanth2011.github.graveldb.server.GravelDBServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

public class Main {
    public static void main(String[] args) throws IOException, RocksDBException {

        Properties properties = new Properties();

        try (InputStream is = Main.class.getClassLoader().getResourceAsStream(args[0].split("=")[1])) {
            if (is != null) {
                properties.load(is);
            }
        }

        String instanceId = properties.getProperty("server.name");
        int port = Integer.parseInt(properties.getProperty("server.port", findRandomOpenPortOnAllLocalInterfaces()));

        String[] targets = properties.getProperty("targets").split(",");
        List<ManagedChannel> channels = Stream.of(targets)
                .map(t -> ManagedChannelBuilder.forTarget(t).usePlaintext().build()).toList();

        Map<String, String> dbConf = Map.of(
                "dataDir", "/Users/revanth/rocksdb/data/" + instanceId + "/dataDir",
                "logDir", "/Users/revanth/rocksdb/" + instanceId + "/log",
                "logMetadataDir", "/Users/revanth/rocksdb/" + instanceId + "/logMetadata");

        GravelDBServer gravelDBServer = new GravelDBServer(instanceId, port, channels, dbConf);
        gravelDBServer.init();
    }

    private static String findRandomOpenPortOnAllLocalInterfaces() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return Integer.toString(socket.getLocalPort());
        }
    }

}
