package app_kvECS;

import ecs.ECSNode;
import shared.messages.KVMessage;
import shared.messages.MessageType;
import shared.messages.UnifiedMessage;

import java.util.concurrent.TimeoutException;

public class ECSRequestsLib {
    /**
     * Throws TimeoutException if request fails
     *
     * @param host
     * @param port
     * @throws TimeoutException
     */
    public static void heartbeat(String host, int port, int timeout) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.ECS_TO_SERVER)
            .withStatusType(KVMessage.StatusType.ECS_HEARTBEAT)
            .build();

        UnifiedMessage resp;
        try {
            TCPSockModule module = new TCPSockModule(host, port, timeout);
            resp = module.doRequest(msg);
            assert(resp.getStatusType().equals(KVMessage.StatusType.SERVER_HEARTBEAT));
        } catch (Exception e) {
            throw e;
        }
    }

}
