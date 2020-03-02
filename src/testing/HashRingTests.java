package testing;

import app_kvECS.HashRing;
import app_kvECS.impl.HashRingImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import ecs.ECSNode;
import ecs.IECSNode;
import junit.framework.TestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import com.google.gson.Gson;
import shared.Pair;
import shared.messages.KVDataSet;
import shared.messages.KVMessage;
import shared.messages.MessageType;
import shared.messages.UnifiedRequestResponse;

import java.lang.reflect.Type;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class HashRingTests extends TestCase {
    private Random rand = new Random();

    @Rule
    public Timeout globalTimeout = new Timeout(10000);

    private ECSNode mockECSNode() {
        return new ECSNode(
            String.format("server-%d", rand.nextInt(Integer.MAX_VALUE)),
            "127.0.0.1",
            rand.nextInt(10000));
    }

    @Test
    public void testBasic() throws Exception {
        final HashRing hashRing = new HashRingImpl();
        ECSNode s1 = mockECSNode();
        ECSNode s2 = mockECSNode();
        ECSNode s3 = mockECSNode();

        hashRing.addServer(s1);
        hashRing.addServer(s2);
        hashRing.addServer(s3);
        hashRing.updateRing();

        Consumer<ECSNode> eval = new Consumer<ECSNode>() {
            @Override
            public void accept(ECSNode s) {
                final String[] range = hashRing.getServerHashRange(s).toArray();
                final HashRing.Hash h = new HashRing.Hash(s.getUuid());
                assert (range[0].equals(h.toHexString()));
            }
        };

        eval.accept(s1);
        eval.accept(s2);
        eval.accept(s3);
    }

    @Test
    public void testBasic2() throws Exception {
        List<ECSNode> servers;

        HashRing hashRing = new HashRingImpl();
        ECSNode s1 = mockECSNode();
        ECSNode s2 = mockECSNode();
        ECSNode s3 = mockECSNode();

        hashRing.addServer(s1);
        hashRing.addServer(s2);
        hashRing.addServer(s3);

        servers = hashRing.filterServer(new Predicate<ECSNode>() {
            @Override
            public boolean test(ECSNode ecs) {
                return ecs.getEcsNodeFlag().equals(IECSNode.ECSNodeFlag.START);
            }
        });
        assert(servers.size() == 0);
        servers = hashRing.filterServer(new Predicate<ECSNode>() {
            @Override
            public boolean test(ECSNode ecs) {
                return ecs.getEcsNodeFlag().equals(IECSNode.ECSNodeFlag.IDLE_START);
            }
        });
        assert(servers.size() == 3);

        hashRing.updateRing();

        servers = hashRing.filterServer(new Predicate<ECSNode>() {
            @Override
            public boolean test(ECSNode ecs) {
                return ecs.getEcsNodeFlag().equals(IECSNode.ECSNodeFlag.START);
            }
        });
        assert(servers.size() == 3);

        hashRing.removeServer(s1);
        hashRing.removeServer(s2);
        hashRing.removeServer(s3);

        servers = hashRing.filterServer(new Predicate<ECSNode>() {
            @Override
            public boolean test(ECSNode ecs) {
                return ecs.getEcsNodeFlag().equals(IECSNode.ECSNodeFlag.START_STOP);
            }
        });
        assert(servers.size() == 3);

        hashRing.updateRing();
        servers = hashRing.filterServer(new Predicate<ECSNode>() {
            @Override
            public boolean test(ECSNode ecs) {
                return ecs.getEcsNodeFlag().equals(IECSNode.ECSNodeFlag.START);
            }
        });
        assert(servers.size() == 0);
    }

    private boolean isSame(Map<String, ECSNode> s1, Map<String, ECSNode> s2) {
        if (s1.size() != s2.size()) {
            return false;
        }

        Iterator<Map.Entry<String, ECSNode>> it = s1.entrySet().iterator();
        Map.Entry<String, ECSNode> entry;
        ECSNode s1_n, s2_n;
        while (it.hasNext()) {
            entry = it.next();
            s1_n = entry.getValue();

            s2_n = s2.get(entry.getKey());
            if (Objects.isNull(s2_n)) {
                return false;
            }

            if (!s1_n.compareTo(s2_n)) {
                return false;
            }
        }

        return true;
    }

    private boolean isSame(TreeMap<HashRing.Hash, ECSNode> s1, TreeMap<HashRing.Hash, ECSNode> s2) {
        if (s1.size() != s2.size()) {
            return false;
        }

        Iterator<Map.Entry<HashRing.Hash, ECSNode>> it = s1.entrySet().iterator();
        Map.Entry<HashRing.Hash, ECSNode> entry;
        ECSNode s1_n, s2_n;
        HashRing.Hash s1_h;
        while (it.hasNext()) {
            entry = it.next();
            s1_h = entry.getKey();
            s1_n = entry.getValue();

            s2_n = s2.get(s1_h);
            if (Objects.isNull(s2_n)) {
                return false;
            }

            if (!s1_n.compareTo(s2_n)) {
                return false;
            }
        }

        return true;
    }

    @Test
    public void testSerialize() throws Exception {
        HashRing hashRing = new HashRingImpl();
        int i;
        for (i=0; i<10; i++) {
            ECSNode node = mockECSNode();
            hashRing.addServer(node);
        }
        hashRing.updateRing();

        String serialized = hashRing.serialize();
        HashRing hashRingCpy = new HashRingImpl().deserialize(serialized);

        assert(isSame(hashRing.getRing(), hashRingCpy.getRing()));
        assert(isSame(hashRing.getServers(), hashRingCpy.getServers()));
    }

    @Test
    public void testUnifiedDS() {
        UnifiedRequestResponse request = new UnifiedRequestResponse.Builder()
            .withMessageType(MessageType.ECS_TO_SERVER)
            .withStatusType(KVMessage.StatusType.START)
            .withKey("Hi")
            .withValue("VALUEEEEEE")
            .withCacheSize(100)
            .withCacheStrategy("FIFO")
            .withKeyRange(new String[]{"asdlfkadcljsadc", "asdflkjadslkcjal"})
            .withServer(mockECSNode())
            .build();

        String json = request.serialize();
        System.out.println(json);
        UnifiedRequestResponse request2 = new UnifiedRequestResponse().deserialize(json);

        assert(request2.getServer().compareTo(request.getServer()));
        assert(request2.getKeyRange()[0].equals(request.getKeyRange()[0]));
        assert(request2.getKeyRange()[1].equals(request.getKeyRange()[1]));
        assert(request2.getCacheSize().equals(request.getCacheSize()));
        assert(request2.getCacheStrategy().equals(request.getCacheStrategy()));
    }

    @Test
    public void testUnifiedObj2() {
        List<Pair<String, String>> entries = new ArrayList<>();
        entries.add(new Pair<>("hi", "there"));
        entries.add(new Pair<>("hello", "man"));
        entries.add(new Pair<>("im", "adfsafd"));
        entries.add(new Pair<>("dsflkjc", "dfkjer"));
        entries.add(new Pair<>("3123lkj", "adsflkjc"));
        entries.add(new Pair<>("97123123", "1028301982312"));

        for (Pair<String, String> entry : entries) {
            System.out.println(entry.toString());
        }
        UnifiedRequestResponse request = new UnifiedRequestResponse.Builder()
            .withMessageType(MessageType.ECS_TO_SERVER)
            .withDataSet(new KVDataSet(entries))
            .build();

        String json = request.serialize();
        System.out.println(json);
        UnifiedRequestResponse request2 = new UnifiedRequestResponse().deserialize(json);

        List<Pair<String, String>> entries2 = request2.getDataSet().getEntries();
        Pair<String, String> pair1;
        Pair<String, String> pair2;
        int i;
        for (i=0; i<entries2.size(); i++) {
            pair1 = entries.get(i);
            pair2 = entries2.get(i);

            System.out.println(pair1.toString());
            System.out.println(pair2.toString());

            assert(pair1.getKey().equals(pair2.getKey()));
            assert(pair1.getValue().equals(pair2.getValue()));
        }
    }
}
