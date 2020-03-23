package app_kvServer;

import app_kvECS.TCPSockModule;
import ecs.ECSNode;
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
