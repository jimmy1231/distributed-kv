package app_kvServer;

import app_kvECS.SocketRequest;
import shared.Pair;

import java.util.List;

public class KVDataTransferRequest implements SocketRequest {
    private List<Pair<String, String>> entries;

    public KVDataTransferRequest(List<Pair<String, String>> entries) {
        this.entries = entries;
    }

    @Override
    public String toJsonString() {
        return null;
    }

    @Override
    public SocketRequest fromJsonString(String json) {
        return null;
    }
}
