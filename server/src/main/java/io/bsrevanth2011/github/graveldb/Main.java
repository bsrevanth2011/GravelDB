package io.bsrevanth2011.github.graveldb;

import io.bsrevanth2011.github.graveldb.server.GravelDBServer;
import io.bsrevanth2011.github.graveldb.server.ServerStubConfig;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {

        int instanceId = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);
        YAMLConfiguration config = new YAMLConfiguration();
        config.read(new InputStreamReader(Main.class.getClassLoader().getResourceAsStream("application.yml")));
        List<HierarchicalConfiguration<ImmutableNode>> peerConfigs = config.configurationsAt("server.stubs");

        List<ServerStubConfig> stubs = new ArrayList<>();
        for (var peerConfig : peerConfigs) {
            int id = peerConfig.getInt("instanceId");
            String target = peerConfig.getString("target");
            if (id == instanceId) continue;
            stubs.add(new ServerStubConfig(id, target));
        }

        Map<String, String> dbConf = Map.of(
                "dataDir", "/Users/revanth/rocksdb/data/" + instanceId + "/dataDir",
                "logDir", "/Users/revanth/rocksdb/" + instanceId + "/log",
                "logMetadataDir", "/Users/revanth/rocksdb/" + instanceId + "/logMetadata");

        GravelDBServer gravelDBServer = new GravelDBServer(instanceId, port, dbConf, stubs.toArray(new ServerStubConfig[0]));
        gravelDBServer.init();
    }
}
