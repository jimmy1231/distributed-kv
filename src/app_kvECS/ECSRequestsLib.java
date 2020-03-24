package app_kvECS;

import ecs.ECSNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shared.messages.KVDataSet;
import shared.messages.KVMessage;
import shared.messages.MessageType;
import shared.messages.UnifiedMessage;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;


public class ECSRequestsLib {
    private static final Logger logger = LoggerFactory.getLogger(ECSRequestsLib.class);

    /**
     * Throws Exception if request fails / times out
     */
    public static void heartbeat(ECSNode server, int timeout) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.ECS_TO_SERVER)
            .withStatusType(KVMessage.StatusType.ECS_HEARTBEAT)
            .build();

        boolean isAlive = false;
        TCPSockModule module;
        UnifiedMessage resp;
        try {
            module = new TCPSockModule(server.getNodeHost(),
                server.getNodePort(), timeout);
            resp = module.doRequest(msg);
            assert(resp.getStatusType().equals(KVMessage.StatusType.SERVER_HEARTBEAT));
            isAlive = true;
        } catch (Exception e) {
            throw e;
        } finally {
            logger.info("[HEARTBEAT]: Server '{}' is {}",
                isAlive ? "ALIVE" : "DEAD");
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
                                          ECSNode oldPrimary,
                                          String[] range) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.ECS_TO_SERVER)
            .withStatusType(KVMessage.StatusType.RECOVER_DATA)
            .withKeyRange(range)
            .withPrimary(oldPrimary)
            .withServer(dest)
            .build();

        lockServer(dest);
        send(src, msg);
        unlockServer(dest);
        logger.info("Success Moved Replicated Data. Replica: '{}' -> Node: '{}'",
            src.getUuid(), dest.getUuid());
    }

    public static void availMoveReplicatedData(List<ECSNode> replicas,
                                               ECSNode dest,
                                               ECSNode oldPrimary,
                                               String[] range,
                                               HashRing hashRing) throws Exception {
        // Choose replica
        ECSNode replica = null;
        for (ECSNode _replica : replicas) {
            try {
                heartbeat(_replica, HeartbeatMonitor.TIMEOUT_MS);
                replica = _replica;
                break;
            } catch (Exception e) {
                /* Swallow */
            }
        }

        // Choose dest
        ECSNode chosenDest = null, _dest = dest;
        do {
            try {
                heartbeat(_dest, HeartbeatMonitor.TIMEOUT_MS);
                chosenDest = _dest;
            } catch (Exception e) {
                /* Swallow */
            }
            _dest = hashRing.getSuccessorServer(_dest);
        } while (Objects.isNull(chosenDest) && !_dest.getUuid().equals(dest.getUuid()));

        // Check if both dest and replica exist
        if (Objects.isNull(replica) || Objects.isNull(chosenDest)) {
            List<String> replicaNames = replicas.stream()
                .map(ECSNode::getUuid)
                .collect(Collectors.toList());

            throw new Exception(String.format(
                "Failed to replicate data to %s, " +
                    "all replicas failed: %s",
                dest.getUuid(), replicaNames));
        }

        // Then, do it
        moveReplicatedData(replica, chosenDest, oldPrimary, range);
    }

    public static void moveData(ECSNode src,
                                ECSNode dest,
                                String[] range) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.ECS_TO_SERVER)
            .withStatusType(KVMessage.StatusType.SERVER_MOVEDATA)
            .withKeyRange(range)
            .withServer(dest)
            .build();

        lockServer(dest);
        send(src, msg);
        unlockServer(dest);
        logger.info("Success Moved Data. Src: '{}' -> Dest: '{}'",
            src.getUuid(), dest.getUuid());
    }

    public static void availMoveData(ECSNode src,
                                     ECSNode dest,
                                     String[] range,
                                     HashRing hashRing) throws Exception {
        /*
         * Note: we only check heartbeat of src node because dest
         * this function is only used in recoverServers for ECSClient,
         * where dest node is the newly created node and assumed
         * to be available (or else it would've failed)
         *
         * If use for any other case, consider implementing a
         * heartbeat for dest server as well.
         */
        try {
            heartbeat(src, HeartbeatMonitor.TIMEOUT_MS);

            // Heartbeat success, src server is alive
            moveData(src, dest, range);
        } catch (Exception e) {
            availMoveReplicatedData(hashRing.getReplicas(src),
                dest, src, range, hashRing);
        }
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
        logger.info("Initialized Server: '{}'", server.getUuid());
    }

    public static void updateServer(ECSNode server,
                                    KVServerMetadata metadata) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.ECS_TO_SERVER)
            .withStatusType(KVMessage.StatusType.SERVER_UPDATE)
            .withMetadata(metadata)
            .build();

        send(server, msg);
        logger.info("Updated Server: '{}'", server.getUuid());
    }

    public static KVDataSet getServerData(ECSNode server) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.ECS_TO_SERVER)
            .withStatusType(KVMessage.StatusType.SERVER_DUMP_DATA)
            .build();

        return send(server, msg).getDataSet();
    }

    public static KVDataSet getServerReplicaData(ECSNode replica,
                                                 ECSNode coordinator) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.ECS_TO_SERVER)
            .withStatusType(KVMessage.StatusType.SERVER_DUMP_REPLICA_DATA)
            .withServer(coordinator)
            .build();

        return send(replica, msg).getDataSet();
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
