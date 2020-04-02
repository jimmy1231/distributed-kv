package app_kvServer;

import ecs.ECSNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shared.messages.KVMessage;

public class MapReduceThread extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(MapReduceThread.class);

    private ECSNode worker;
    private String partId;
    private String resultId;
    private KVMessage.StatusType mapOrReduce;

    public MapReduceThread(ECSNode worker, String partId,
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
    }

    @Override
    public void run() {
        String resultId;
        try {
            logger.info("[MAPPER_THREAD]: Mapper='{}', MapId='{}'",
                worker.getUuid(), partId);

            switch (mapOrReduce) {
                case MAP:
                    resultId = KVServerRequestLib.serverDoMap(worker, partId);
                    this.resultId = resultId;
                    break;
                case REDUCE:
                    resultId = KVServerRequestLib.serverDoReduce(worker, partId);
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
