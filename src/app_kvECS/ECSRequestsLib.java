package app_kvECS;

import ecs.ECSNode;
import ecs.IECSNode;
import shared.messages.KVMessage;
import shared.messages.MessageType;
import shared.messages.UnifiedMessage;

import java.util.Objects;
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

        TCPSockModule module;
        UnifiedMessage resp;
        try {
            module = new TCPSockModule(host, port, timeout);
            resp = module.doRequest(msg);
            assert(resp.getStatusType().equals(KVMessage.StatusType.SERVER_HEARTBEAT));
        } catch (Exception e) {
            throw e;
        }
    }

    public static void lockServer(ECSNode server) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.ECS_TO_SERVER)
            .withStatusType(KVMessage.StatusType.SERVER_WRITE_LOCK)
            .build();

        send(server, msg);
    }

    public static void unlockServer(ECSNode server) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.ECS_TO_SERVER)
            .withStatusType(KVMessage.StatusType.SERVER_WRITE_UNLOCK)
            .build();

        send(server, msg);
    }

    public static void moveReplicatedData(ECSNode src,
                                          ECSNode dest,
                                          HashRing.HashRange range) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.ECS_TO_SERVER)
            .withStatusType(KVMessage.StatusType.REPLICATE_DATA)
            .withKeyRange(range.toArray())
            .withServer(dest)
            .build();

        lockServer(dest);
        send(src, msg);
        unlockServer(dest);
    }

    public static void moveData(ECSNode src,
                                ECSNode dest,
                                HashRing.HashRange range) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.ECS_TO_SERVER)
            .withStatusType(KVMessage.StatusType.SERVER_MOVEDATA)
            .withKeyRange(range.toArray())
            .withServer(dest)
            .build();

        lockServer(dest);
        send(src, msg);
        unlockServer(dest);
    }

    public static void initServer(ECSNode server,
                                  String cacheStrategy,
                                  int cacheSize,
                                  KVServerMetadata metadata) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.ECS_TO_SERVER)
            .withStatusType(KVMessage.StatusType.SERVER_INIT)
            .withCacheSize(cacheSize)
            .withCacheStrategy(cacheStrategy)
            .withMetadata(metadata)
            .build();

        send(server, msg);
    }

    public static void updateServerReplicate(ECSNode server,
                                             KVServerMetadata metadata) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.ECS_TO_SERVER)
            .withStatusType(KVMessage.StatusType.SERVER_REPLICATE_UPDATE)
            .withMetadata(metadata)
            .build();

        send(server, msg);
    }

    private static UnifiedMessage send(ECSNode server, UnifiedMessage msg) throws Exception {
        return send(server.getNodeHost(), server.getNodePort(), msg);
    }

    private static UnifiedMessage send(String host, int port, UnifiedMessage msg) throws Exception {
        TCPSockModule module = null;
        UnifiedMessage resp = null;
        try {
            module = new TCPSockModule(host, port);
            resp = module.doRequest(msg);
        } catch (Exception e) {
            throw e;
        } finally {
            if (Objects.nonNull(module)) {
                module.close();
            }
        }

        return resp;
    }
}
