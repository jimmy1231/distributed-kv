package server;

import ecs.HashRing;
import shared.Pair;

public class DFS {
    private HashRing ring;

    public DFS(HashRing ring) {
        this.ring = ring;
    }

    public String get(String id) throws Exception {
        return ServerRequestLib.serverGetKV(ring, id).getValue();
    }

    public void put(Pair<String, String> entry) throws Exception {
        ServerRequestLib.serverPutKV(ring, entry);
    }
}
