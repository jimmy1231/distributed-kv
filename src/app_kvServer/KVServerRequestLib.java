package app_kvServer;

import app_kvECS.HashRing;
import app_kvECS.TCPSockModule;
import ecs.ECSNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shared.Pair;
import shared.messages.KVDataSet;
import shared.messages.KVMessage;
import shared.messages.MessageType;
import shared.messages.UnifiedMessage;

import java.util.Objects;
import java.util.UUID;

public class KVServerRequestLib {
    private static final Logger logger = LoggerFactory.getLogger(KVServerRequestLib.class);

    public static void replicaRecoverData(ECSNode replicaServer,
                                          ECSNode dest,
                                          KVDataSet dataSet) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.SERVER_TO_SERVER)
            .withServer(replicaServer)
            .withStatusType(KVMessage.StatusType.RECOVER_DATA)
            .withDataSet(dataSet)
            .build();

        send(dest, msg);
    }

    public static Pair<String, String> serverGetKV(HashRing ring,
                                                   String key) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.SERVER_TO_SERVER)
            .withStatusType(KVMessage.StatusType.GET)
            .withKey(key)
            .build();

        ECSNode server = ring.getServerByObjectKey(key);
        UnifiedMessage resp = send(server, msg);
        logger.info("Success Server getKV " +
                "-> {}, Key=<{}>, Data='{}'",
            server.getUuid(), key, resp.getValue());

        return new Pair<>(key, resp.getValue());
    }

    public static void serverPutKV(HashRing ring,
                                   Pair<String, String> entry) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.SERVER_TO_SERVER)
            .withStatusType(KVMessage.StatusType.PUT_DATA)
            .withKey(entry.getKey())
            .withValue(entry.getValue())
            .withUUID(UUID.randomUUID())
            .build();

        ECSNode server = ring.getServerByObjectKey(entry.getKey());
        send(server, msg);
        logger.info("Success Server putKV " +
                "-> {}, Key=<{}>, Data='{}'",
            server.getUuid(),
            entry.getKey(), entry.getValue());
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
