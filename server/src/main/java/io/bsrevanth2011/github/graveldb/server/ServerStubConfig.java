package io.bsrevanth2011.github.graveldb.server;

public record ServerStubConfig(int instanceId, String target, int nextIndex) {

    public ServerStubConfig(int instanceId, String target) {
        this(instanceId, target, 1);
    }

}
