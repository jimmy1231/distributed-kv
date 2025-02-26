package server;

import server.dsmr.MapReduce;
import ecs.ECSNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shared.messages.KVMessage;

public class MapReduceThread extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(MapReduceThread.class);

    private ECSNode worker;
    private String partId;
    private String resultId;
    private MapReduce.Type mrType;
    private KVMessage.StatusType mapOrReduce;

    public MapReduceThread(ECSNode worker, String partId,
                           MapReduce.Type mrType,
                           KVMessage.StatusType mapOrReduce) throws Exception {
        switch (mapOrReduce) {
            case MAP:
            case REDUCE:
                break;
            default:
                throw new Exception("Can only be MAP or REDUCE");
        }

        this.worker = worker;
        this.partId = partId;
        this.mapOrReduce = mapOrReduce;
        this.mrType = mrType;
    }

    @Override
    public void run() {
        String resultId;
        try {
            logger.info("[MAPPER_THREAD]: Mapper='{}', MapId='{}'",
                worker.getUuid(), partId);

            switch (mapOrReduce) {
                case MAP:
                    resultId = ServerRequestLib.serverDoMap(worker, partId, mrType);
                    this.resultId = resultId;
                    break;
                case REDUCE:
                    resultId = ServerRequestLib.serverDoReduce(worker, partId, mrType);
                    this.resultId = resultId;
                    break;
                default:
            }

            logger.info("[MAPPER_THREAD]: {} FINISHED " +
                    "- Worker='{}', PartId='{}'",
                mapOrReduce, worker.getUuid(), partId);
        } catch (Exception e) {
            // interrupt self
            interrupt();
        }
    }

    public ECSNode getWorker() {
        return worker;
    }

    public MapReduceThread setWorker(ECSNode worker) {
        this.worker = worker;
        return this;
    }

    public String getPartId() {
        return partId;
    }

    public MapReduceThread setPartId(String partId) {
        this.partId = partId;
        return this;
    }

    public String getResultId() {
        return resultId;
    }

    public MapReduceThread setResultId(String resultId) {
        this.resultId = resultId;
        return this;
    }
}
