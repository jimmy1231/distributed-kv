package ecs;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class ECSNode implements IECSNode {
    @Expose
    private String uuid = ""; // ${host}:${name}
    @Expose
    private String name = "";
    @Expose
    private String host = "";
    @Expose
    private int port = -1;
    @Expose
    private String[] range = null;
    @Expose
    private ECSNodeFlag ecsNodeFlag;

    public ECSNode(String name, String host, int port) {
        this.uuid = String.format("%s:%d", host, port);
        this.name = name;
        this.host = host;
        this.port = port;
        this.ecsNodeFlag = ECSNodeFlag.IDLE;
    }

    public String getUuid() {
        return uuid;
    }

    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName(){
        return name;
    }

    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost() {
        return host;
    }

    /**
     * @return  the port number of the node (ie 8080)
     */
    public int getNodePort() {
        return port;
    }

    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public String[] getNodeHashRange() {
        return range;
    }


    public ECSNodeFlag getEcsNodeFlag() {
        return ecsNodeFlag;
    }

    public void setEcsNodeFlag(ECSNodeFlag ecsNodeFlag) {
        this.ecsNodeFlag = ecsNodeFlag;
    }

    public void setNodeHashRange(String[] range) {
        this.range = range;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    public boolean compareTo(ECSNode o) {
        if (Objects.isNull(range)) {
            return uuid.equals(o.uuid)
                && name.equals(o.name)
                && host.equals(o.host)
                && port == o.port
                && ecsNodeFlag.equals(o.ecsNodeFlag);
        }

        return uuid.equals(o.uuid)
            && name.equals(o.name)
            && host.equals(o.host)
            && port == o.port
            && range[0].equals(o.range[0])
            && range[1].equals(o.range[1])
            && ecsNodeFlag.equals(o.ecsNodeFlag);
    }
}
