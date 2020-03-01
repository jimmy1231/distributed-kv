package app_kvServer;

import app_kvECS.SocketResponse;

public class KVDataTransferResponse implements SocketResponse {
    @Override
    public String toJsonString() {
        return null;
    }

    @Override
    public SocketResponse fromJsonString(String json) {
        return null;
    }
}
