package ecs;

import java.util.Collection;
import java.util.Map;

public class ECSNode implements IECSNode {
    String name = "";
    String host = "";
    int port = -1;
    String[] range = null;

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

}
