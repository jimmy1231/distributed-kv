package testing;

import app_kvECS.HashRing;
import app_kvECS.KVServerMetadata;
import app_kvECS.impl.HashRingImpl;
import app_kvECS.impl.KVServerMetadataImpl;
import app_kvServer.IKVServer;
import app_kvServer.KVServer;
import ecs.ECSNode;
import ecs.IECSNode;
import junit.framework.TestCase;
import shared.messages.KVMessage;

public class ReplicationTests extends TestCase {
    private KVServer server;
    private HashRing hashRing;
    private KVServerMetadata metadata;

    public void setUp() {
        server = new KVServer(50000, 1000, "FIFO");
        hashRing = new HashRingImpl();
        ECSNode s1 = new ECSNode("Server1", "localhost", 50000);
        ECSNode s2 = new ECSNode("Server2", "localhost", 50001);
        ECSNode s3 = new ECSNode("Server3", "localhost", 50002);

        s1.setEcsNodeFlag(IECSNode.ECSNodeFlag.IDLE_START);
        s2.setEcsNodeFlag(IECSNode.ECSNodeFlag.IDLE_START);
        s3.setEcsNodeFlag(IECSNode.ECSNodeFlag.IDLE_START);

        hashRing.addServer(s1);
        hashRing.addServer(s2);
        hashRing.addServer(s3);
        hashRing.updateRing();

        metadata = new KVServerMetadataImpl("Server1", "localhost", IECSNode.ECSNodeFlag.START, hashRing);
        server.update(metadata);
    }

    public void testGetReplicas() {

    }
}
