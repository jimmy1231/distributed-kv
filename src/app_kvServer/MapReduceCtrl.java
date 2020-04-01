package app_kvServer;

import app_kvECS.HashRing;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shared.Pair;
import shared.messages.KVDataSet;
import shared.messages.MessageType;
import shared.messages.UnifiedMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static shared.messages.KVMessage.StatusType.ERROR;
import static shared.messages.KVMessage.StatusType.SUCCESS;

public class MapReduceCtrl {
    private static final Logger logger = LoggerFactory.getLogger(MapReduceCtrl.class);

    public static String[] masterMapReduce(HashRing ring, String[] keys) throws Exception {
        /*
         * (1) Get KV pairs for each key
         * (2) Combine value, split into M arbitrary parts, where M
         *     is the number of available worker nodes - can be in
         *     any STATE, as long as they are not SHUTDOWN (e.g.
         *     not running)
         * (3) Puts each part back into DFS
         */
        KVDataSet dataSet = new KVDataSet();

        // (1)
        {
            String key;
            int i;
            for (i = 0; i < keys.length; i++) {
                key = keys[i];
                try {
                    dataSet.addEntry(KVServerRequestLib.serverGetKV(ring, key));
                } catch (Exception e) {
                    logger.error("Could not get data with key: {}", key, e);
                    /* Swallow */
                }
            }
        }

        // (2)
        List<String> parts = new ArrayList<>();
        {
            String combined = dataSet.combineValues();
            logger.debug("[MAP_REDUCE]: Combined: {}", combined);
            String[] split = combined.split(" ");

            int M = ring.getNumActiveServers() - 1; // exclude current server (master)
            int numPerPart = split.length / M; // truncates down

            int startInd, endInd;
            int i;
            for (i=0; i<M ;i++) {
                startInd = i*numPerPart;
                if (i+1 == M) {
                    endInd = split.length;
                } else {
                    endInd = startInd + numPerPart;
                }

                String[] _arr = ArrayUtils.subarray(split,startInd,endInd);
                parts.add(String.join(" ", _arr));
            }
            logger.info("[MAP_REDUCE]: Parts: {}", parts);
        }

        // (3)
        List<String> mapIds = new ArrayList<>();
        {
            String partKey;
            Pair<String, String> entry;
            for (String part : parts) {
                partKey = UUID.randomUUID().toString();
                entry = new Pair<>(partKey, part);
                try {
                    KVServerRequestLib.serverPutKV(ring, entry);
                } catch (Exception e) {
                    logger.error("[MAP_REDUCE]: Could not put data", entry, e);
                    // TODO: provide fault tolerance
                    /* Swallow */
                }

                mapIds.add(partKey);
            }
        }

        // (4)
        MapReduceCtrl.doMap(ring, mapIds);

        return new String[0];
    }

    /**
     * Given the ids of the parts to be mapped, delegate
     * Mappers to process these requests. Only the Master should
     * invoke this function; the Master should not be part
     * of the Mappers.
     *
     * Also responsible for failure handling if any workers
     * fail by starting a Map task in an available server.
     *
     * Returns a list of ids corresponding to the Mapped
     * results.
     *
     * @param mapIds
     * @return
     */
    private static String[] doMap(HashRing ring, List<String> mapIds) {
        logger.info("DO MAP: {}", mapIds);

        // Everything from step 5-7
        return null;
    }
}
