package testing;

import app_kvECS.HashRing;
import app_kvECS.impl.HashRingImpl;
import ecs.ECSNode;
import junit.framework.TestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Random;

public class HashRingTests extends TestCase {
    private Random rand = new Random();

    @Rule
    public Timeout globalTimeout = new Timeout(10000);

    private ECSNode mockECSNode() {
        return new ECSNode(
            String.format("server-%d", rand.nextInt(1000)),
            "127.0.0.1",
            rand.nextInt(10000));
    }

    @Test
    public void testBasic() throws Exception {
        HashRing hashRing = new HashRingImpl();
        ECSNode s1 = mockECSNode();
        ECSNode s2 = mockECSNode();
        ECSNode s3 = mockECSNode();

        hashRing.addServer(s1);
        hashRing.addServer(s2);
        hashRing.addServer(s3);
        hashRing.updateRing();

        String[] range = hashRing.getServerHashRange(s1).toArray();
        HashRing.Hash h = new HashRing.Hash(s1.getUuid());
        assert(range[0].equals(new HashRing.Hash(s1.getUuid()).toString()));
    }
}
