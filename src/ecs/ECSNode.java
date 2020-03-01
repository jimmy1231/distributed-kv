package ecs;

import java.util.Collection;
import java.util.Map;

public class ECSNode implements IECSNode {
    private String uuid = ""; // ${host}:${name}
    private String name = "";
    private String host = "";
    private int port = -1;
    private String[] range = null;
    private ECSNodeFlag ecsNodeFlag;

    public ECSNode(String name, String host, int port) {
        this.uuid = String.format("%s:%d", host, port);
        this.name = name;
        this.host = host;
        this.port = port;
        this.ecsNodeFlag = ECSNodeFlag.STOP;
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
}
