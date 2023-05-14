package io.bsrevanth2011.github.graveldb;

import io.bsrevanth2011.github.graveldb.server.GravelDBServer;

import java.io.IOException;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        List<String> targets = List.of("localhost:8080", "localhost:8082");
        GravelDBServer gravelDBServer = new GravelDBServer(8081);
        gravelDBServer.start(targets);
    }
}
