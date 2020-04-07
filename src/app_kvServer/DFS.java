package app_kvServer;

import app_kvECS.HashRing;
import shared.Pair;

public class DFS {
    private HashRing ring;

    public DFS(HashRing ring) {
        this.ring = ring;
    }

    public String get(String id) throws Exception {
        return KVServerRequestLib.serverGetKV(ring, id).getValue();
    }

    public void put(Pair<String, String> entry) throws Exception {
        KVServerRequestLib.serverPutKV(ring, entry);
    }
}
