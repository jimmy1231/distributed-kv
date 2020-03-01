package app_kvECS;

import app_kvECS.impl.ECSSocketsModuleImpl;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.Socket;

public abstract class ECSSocketsModule {
    private static Logger logger = Logger.getLogger(ECSSocketsModule.class);

    protected Socket socket;

    /****************************************************/
    public ECSSocketsModule(String host, int port) throws IOException {
        try {
            socket = new Socket(host, port);
        } catch (IOException e) {
            logger.error("Error initializing sockets module", e);
            throw e;
        }

        logger.info(String.format(
            "ECSSocket connection established: %s:%d",
            host, port));
    }
    /****************************************************/


    //////////////////////////////////////////////////////////////
    public static ECSSocketsModule build(String host, int port) throws Exception {
        return new ECSSocketsModuleImpl(host, port);
    }

    /**
     * Do request. Blocks until response is received.
     *
     */
    public abstract KVAdminResponse doRequest(KVAdminRequest request) throws Exception;
    //////////////////////////////////////////////////////////////
}
