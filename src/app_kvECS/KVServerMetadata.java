package app_kvECS;

import app_kvServer.ServerStatusType;
import ecs.IECSNode;


public abstract class KVServerMetadata {
    protected String name;
    protected String host;
    protected String port;
    protected IECSNode.ECSNodeFlag ecsNodeFlag;
    protected HashRing hashRing;

    /****************************************************/
    public KVServerMetadata(String name, String host, IECSNode.ECSNodeFlag ecsNodeFlag, HashRing hashRing) {
        this.name = name;
        this.host = host;
        this.hashRing = hashRing;
        this.ecsNodeFlag = ecsNodeFlag;
    }
    /****************************************************/

    //////////////////////////////////////////////////////////////
    public abstract String getName();
    public abstract String getHost();
    public abstract HashRing getHashRing();
    public abstract IECSNode.ECSNodeFlag getEcsNodeFlag();

    public abstract String serialize();
    //////////////////////////////////////////////////////////////
}
