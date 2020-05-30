package ecs;


public abstract class ServerMetadata {
    protected String name;
    protected String host;
    protected String port;
    protected IECSNode.ECSNodeFlag ecsNodeFlag;
    protected HashRing hashRing;

    /****************************************************/
    public ServerMetadata() {
        /* Default constructor */
    }

    public ServerMetadata(String name, String host, IECSNode.ECSNodeFlag ecsNodeFlag, HashRing hashRing) {
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
    public abstract void setECSNodeFlag(IECSNode.ECSNodeFlag flag);

    public abstract String serialize();
    public abstract ServerMetadata deserialize(String string);
    //////////////////////////////////////////////////////////////
}
