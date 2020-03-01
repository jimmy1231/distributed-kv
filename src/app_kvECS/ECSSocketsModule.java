package app_kvECS;

import java.net.Socket;

public abstract class ECSSocketsModule {
    protected Socket socket;

    /**
     * Do request. Blocks until response is received.
     *
     */
    public abstract KVAdminResponse doRequest(KVAdminRequest request);
}
