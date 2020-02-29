package app_kvECS;

import app_kvServer.ServerStatusType;


public abstract class KVServerMetadata {
    protected String name;
    protected String host;
    protected ServerStatusType serverStatusType;

    /****************************************************/
    public KVServerMetadata(String name, String host, ServerStatusType serverStatusType) {
        this.name = name;
        this.host = host;
        this.serverStatusType = serverStatusType;
    }
    /****************************************************/

    //////////////////////////////////////////////////////////////
    public abstract String getName();
    public abstract String getHost();
    public abstract ServerStatusType getServerStatusType();

    public abstract String serialize();
    //////////////////////////////////////////////////////////////
}
