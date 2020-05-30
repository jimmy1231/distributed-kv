package testing;

import app_kvECS.HashRing;
import app_kvECS.ServerMetadata;
import app_kvECS.impl.HashRingImpl;
import app_kvECS.impl.ServerMetadataImpl;
import app_kvServer.Server;
import app_kvECS.ECSNode;
import app_kvECS.IECSNode;
import junit.framework.TestCase;

public class ReplicationTests extends TestCase {
    private Server server;
    private HashRing hashRing;
    private ServerMetadata metadata;

    public void setUp() {
        server = new Server(50000, 1000, "FIFO");
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

        metadata = new ServerMetadataImpl("Server1", "localhost", IECSNode.ECSNodeFlag.START, hashRing);
        server.update(metadata);
    }

    public void testGetReplicas() {

    }
}
