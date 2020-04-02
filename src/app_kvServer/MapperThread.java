package app_kvServer;

import ecs.ECSNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapperThread extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(MapperThread.class);

    private ECSNode mapper;
    private String mapId;
    private String mapResultId;

    public MapperThread(ECSNode mapper, String mapId) {
        this.mapper = mapper;
        this.mapId = mapId;
    }

    @Override
    public void run() {
        String mapResult;
        try {
            logger.info("[MAPPER_THREAD]: Mapper='{}', MapId='{}'",
                mapper.getUuid(), mapId);
            mapResult = KVServerRequestLib.serverDoMap(mapper, mapId);
            mapResultId = mapResult;
            logger.info("[MAPPER_THREAD]: FINISHED - Mapper='{}', MapId='{}'",
                mapper.getUuid(), mapId);
        } catch (Exception e) {
            // interrupt self
            interrupt();
        }
    }

    public ECSNode getMapper() {
        return mapper;
    }

    public MapperThread setMapper(ECSNode mapper) {
        this.mapper = mapper;
        return this;
    }

    public String getMapId() {
        return mapId;
    }

    public MapperThread setMapId(String mapId) {
        this.mapId = mapId;
        return this;
    }

    public String getMapResultId() {
        return mapResultId;
    }

    public MapperThread setMapResultId(String mapResultId) {
        this.mapResultId = mapResultId;
        return this;
    }
}
