package io.bsrevanth2011.github.graveldb;

import io.bsrevanth2011.github.graveldb.db.RocksDBService;
import io.bsrevanth2011.github.graveldb.server.GravelDBServer;
import io.bsrevanth2011.github.graveldb.server.ServerStubConfig;
import org.apache.commons.configuration2.YAMLConfiguration;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Main {
    public static void main(String[] args) throws Exception {

        int instanceId = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);

        List<ServerStubConfig> stubConfigs = new ArrayList<>();

        YAMLConfiguration config = new YAMLConfiguration();
        try (InputStream configFile = Main.class.getClassLoader().getResourceAsStream("application.yml")) {

            Objects.requireNonNull(configFile);
            try (InputStreamReader reader = new InputStreamReader(configFile)) {

                config.read(reader);
                var peerConfigs = config.configurationsAt("server.stubs");
                for (var peerConfig : peerConfigs) {
                    int id = peerConfig.getInt("instanceId");
                    String target = peerConfig.getString("target");
                    if (id == instanceId) continue;
                    stubConfigs.add(new ServerStubConfig(id, target));
                }
            }
        }

        RocksDBService.init("/Users/revanth/rocksdb/" + instanceId);
        GravelDBServer gravelDBServer = new GravelDBServer(instanceId, port, stubConfigs.toArray(new ServerStubConfig[0]));
        gravelDBServer.init();
    }
}
