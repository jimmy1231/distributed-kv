package app_kvServer;

import app_kvECS.HashRing;
import app_kvServer.dsmr.MapOutput;
import app_kvServer.dsmr.ReduceInput;
import ecs.ECSNode;
import ecs.IECSNode;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shared.Pair;
import shared.messages.KVDataSet;
import shared.messages.KVMessage;

import java.util.*;
import java.util.function.Function;

import static ecs.IECSNode.ECSNodeFlag.*;
import static java.lang.Math.*;
import static shared.messages.KVMessage.StatusType.*;

public class MapReduceCtrl {
    private static final Logger logger = LoggerFactory.getLogger(MapReduceCtrl.class);
    private static final int SZ_PARTITION = 128; // words
    private static final int SZ_REDUCE = 128; // words

    public static String[] masterMapReduce(ECSNode master,
                                           HashRing ring,
                                           String[] keys) throws Exception {
        /*
         * (1) Get KV pairs for each key
         * (2) Combine value, split into parts. The number of splits
         *     is defined such that the maximum number of splits is M,
         *     where M is the number of available Mappers, and the
         *     minimum split size is SZ_PARTITION. This gives rise
         *     to the following formula:
         *
         *     Let M be the number of available Mappers
         *     Let L be the number of total words
         *     Let S be recommended size (words) of the partition: SZ_PARTITION
         *
         *     Then, the partition size P is defined as:
         *              P = min(L, max(S, L/M))
         *
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
        List<String> mapParts = new ArrayList<>();
        {
            String combined = dataSet.combineValues();
            logger.debug("[MAP_REDUCE]: Combined: {}", combined);
            String[] split = combined.split(" ");
            int M = ring.getNumActiveServers()-1;
            int L = split.length;
            int partSize = min(L, max(SZ_PARTITION, L/M)); // bytes

            int startInd, endInd;
            String[] _arr;
            int numLeft = L;
            int i=0;
            while (numLeft > 0) {
                startInd = i*partSize;
                endInd = min(L, startInd+partSize);

                _arr = ArrayUtils.subarray(split,startInd,endInd);
                mapParts.add(String.join(" ", _arr));

                numLeft -= (endInd-startInd+1);
                i++;
            }

            logger.info("[MAP_REDUCE]: Parts: {}", mapParts);
        }

        // (3-4)
        String[] mapResults = MapReduceCtrl.doMR(
            master, ring,
            putParts(ring, mapParts, MAP),
            MAP);

        // (9)
        List<String> reduceParts = new ArrayList<>();
        {
            KVDataSet mapOutputSet = new KVDataSet();
            Pair<String, String> resultPair;
            MapOutput mapOutput;
            for (String mapResultId : mapResults) {
                try {
                    resultPair = KVServerRequestLib.serverGetKV(ring, mapResultId);
                    assert(resultPair.getKey().equals(mapResultId));
                    mapOutput = new MapOutput(resultPair.getValue());

                    mapOutputSet.merge(mapOutput.getDataSet());
                } catch (Exception e) {
                    logger.error("[MAP_REDUCE]: Failed to " +
                            "retrieve map result: {}",
                        mapResultId, e);
                    /* Swallow */
                }
            }

            // sort mapOutputSet
            mapOutputSet.sortByKeys(true);
            logger.info("[MAP_REDUCE]: Map results sorted: {}",
                mapOutputSet.toString());

            /*
             * Put entries that have the same keys in the same
             * 'bin', assign the values as a ',' delimited string
             * where each entry is the mapped result.
             *
             * The format is:
             * <KEY=uuid, VALUE="mapKey mapValue,mapValue,...">
             *     e.g. <"1asdf1223-12312dfas", "and 1,1,1,1,1">
             */
            List<Pair<String, String>> entries = mapOutputSet.getEntries();
            Stack<ReduceInput> intermed = new Stack<>();
            String lastKey = entries.size() > 0
                ? entries.get(0).getKey()
                : null;

            ReduceInput input = new ReduceInput(lastKey, true);
            for (Pair<String,String> entry : entries) {
                if (entry.getKey().equals(lastKey)) {
                    input.addValue(entry.getValue());
                } else {
                    intermed.push(input);
                    input = new ReduceInput(entry.getKey(), true); // new key
                    input.addValue(entry.getValue());
                }

                lastKey = entry.getKey();
            }

            /*
             * Space optimization:
             * Since values for Reduce are often small, it is beneficial
             * to combine many Reduce operations into a batch for one
             * Reducer to process together. This saves network bandwidth
             * as well as storage space.
             */
            while (!intermed.empty()) {
                ReduceInput.ReduceDTO dto = new ReduceInput.ReduceDTO();
                int i = 0;
                while (!intermed.empty() && i<SZ_REDUCE) {
                    dto.addInput(intermed.pop());
                    i++;
                }
                reduceParts.add(dto.toString());
            }

            // Map operation finished, delete map map results
            List<String> listMapResults = Arrays.asList(mapResults);
            logger.info("[MAP_REDUCE]: Deleting all map results: {}",
                listMapResults);
            KVServerRequestLib.serverDeleteAll(
                ring, listMapResults);
        }

        // (10)
        return MapReduceCtrl.doMR(
            master, ring,
            putParts(ring, reduceParts, REDUCE),
            REDUCE);
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
     * @param partIds
     * @return
     */
    private static String[] doMR(ECSNode master,
                                 HashRing ring,
                                 final List<String> partIds,
                                 final KVMessage.StatusType TYPE) throws Exception {
        logger.info("DO {}: {}", TYPE, partIds);

        int nodeIdx = -1;
        List<ECSNode> availNodes = ring.filterServer(s ->
            !s.getEcsNodeFlag().equals(SHUT_DOWN)
                && !s.getEcsNodeFlag().equals(IDLE)
                && !s.getUuid().equals(master.getUuid())
        );

        Function<Integer, ECSNode> getServer = (n) -> {
            int _n = n % availNodes.size();
            return availNodes.get(_n);
        };

        List<MapReduceThread> threadPool = new ArrayList<>();
        {
            MapReduceThread mt;
            ECSNode mapper;
            for (String mapId : partIds) {
                mapper = getServer.apply(nodeIdx+1);
                mt = new MapReduceThread(mapper, mapId, TYPE);
                mt.start();

                threadPool.add(mt);
                nodeIdx++;
            }
        }

        List<String> mapResults = new ArrayList<>();
        {
            final int MAX_ITERS = 2;
            int iters = 0;
            List<MapReduceThread> leftOvers = new ArrayList<>();
            while (!threadPool.isEmpty() && iters < MAX_ITERS) {
                for (MapReduceThread mt : threadPool) {
                    try {
                        mt.join();
                        mapResults.add(mt.getResultId());
                    } catch (InterruptedException e) {
                        // Retry mapper with new available node
                        availNodes.remove(mt.getWorker());
                        ECSNode mapper = getServer.apply(nodeIdx + 1);
                        MapReduceThread _mt = new MapReduceThread(
                            mapper, mt.getPartId(), TYPE);
                        _mt.start();

                        leftOvers.add(_mt);
                    }
                }

                // Swap
                threadPool = leftOvers;
                leftOvers = new ArrayList<>();
                iters++;
            }
        }

        // Map operation finished, delete map partitions
        KVServerRequestLib.serverDeleteAll(ring, partIds);

        if (mapResults.size() != partIds.size()) {
            throw new Exception(String.format(
                "Map failed, not all tasks finished: " +
                    "Expecting: %s. Got: %s",
                partIds.size(), mapResults.size())
            );
        }

        logger.info("[MASTER_MAP_REDUCE]: {} Success: {}",
            TYPE, mapResults);
        return mapResults.toArray(new String[0]);
    }

    private static List<String> putParts(HashRing ring,
                                         List<String> parts,
                                         KVMessage.StatusType TYPE) {
        List<String> partIds = new ArrayList<>();

        String partKey;
        Pair<String, String> entry;
        for (String part : parts) {
            partKey = String.format("%s-%s",
                TYPE, UUID.randomUUID().toString());
            entry = new Pair<>(partKey, part);
            try {
                KVServerRequestLib.serverPutKV(ring, entry);
            } catch (Exception e) {
                logger.error("[MAP_REDUCE]: Could not put data", entry, e);
                // TODO: provide fault tolerance
                /* Swallow */
            }

            partIds.add(partKey);
        }

        return partIds;
    }
}
