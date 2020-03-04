package testing;

import app_kvECS.TCPSockModule;
import app_kvECS.HashRing;
import app_kvECS.KVServerMetadata;
import app_kvECS.impl.HashRingImpl;
import app_kvECS.impl.KVServerMetadataImpl;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ecs.ECSNode;
import ecs.IECSNode;
import shared.messages.KVMessage;
import shared.messages.MessageType;
import shared.messages.UnifiedMessage;

import java.util.Random;

public class SocketClient {
    private static HashRing getRing(int size) {
        HashRing ring = new HashRingImpl();
        ECSNode ecsNode;
        final int basePort = 30000;
        for (int i=0; i<size; i++) {
            ecsNode = new ECSNode(
                String.format("server-%d", i),
                "127.0.0.1",
                basePort+i);

            ring.addServer(ecsNode);
            ecsNode.setEcsNodeFlag(IECSNode.ECSNodeFlag.IDLE_START);
        }

        return ring;
    }

    private static KVServerMetadata getMetadata() {
        ECSNode ecsNode = new ECSNode(
            String.format("server-%d", 100),
            "127.0.0.1",
            31232);

        return new KVServerMetadataImpl(
            ecsNode.getNodeName(),
            ecsNode.getNodeHost(),
            IECSNode.ECSNodeFlag.START,
            getRing(10)
        );
    }

    public static void main(String[] args) {
        TCPSockModule module = null;
        Gson gson = new GsonBuilder()
            .enableComplexMapKeySerialization()
            .setPrettyPrinting()
            .excludeFieldsWithoutExposeAnnotation()
            .create();
        try {
            module = new TCPSockModule("localhost", 50001);

            KVServerMetadata metadata = getMetadata();
            UnifiedMessage req, resp;
            req = new UnifiedMessage.Builder()
                .withMessageType(MessageType.ECS_TO_SERVER)
                .withStatusType(KVMessage.StatusType.START)
                .withKey("Hi")
                .withValue("There")
                .withMetadata(metadata)
                .build();

            metadata.getHashRing().updateRing();
            metadata.getHashRing().print();
            resp = module.doRequest(req);
            System.out.println(gson.toJson(resp));
            module.close();
            while(true);
        } catch (Exception e) {
            System.out.println("We failed");
        }
    }
}
