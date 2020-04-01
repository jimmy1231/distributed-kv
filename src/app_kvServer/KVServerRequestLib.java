package app_kvServer;

import app_kvECS.HashRing;
import app_kvECS.TCPSockModule;
import ecs.ECSNode;
import shared.Pair;
import shared.messages.KVDataSet;
import shared.messages.KVMessage;
import shared.messages.MessageType;
import shared.messages.UnifiedMessage;

import java.util.Objects;

public class KVServerRequestLib {
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

        UnifiedMessage resp = send(ring.getServerByObjectKey(key), msg);
        return new Pair<>(key, resp.getValue());
    }

    public static void serverPutKV(HashRing ring,
                                   Pair<String, String> entry) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.SERVER_TO_SERVER)
            .withStatusType(KVMessage.StatusType.PUT_DATA)
            .withKey(entry.getKey())
            .withValue(entry.getValue())
            .build();

        send(ring.getServerByObjectKey(entry.getKey()), msg);
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
