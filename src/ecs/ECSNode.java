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

    public boolean start() throws Exception {
        return false;
    }


    public boolean stop() throws Exception {
        return false;
    }

    public boolean shutdown() throws Exception {
        return false;
    }

    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        return null;
    }

    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        return null;
    }

    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        return null;
    }

    public boolean awaitNodes(int count, int timeout) throws Exception {
        return false;
    }


    public boolean removeNodes(Collection<String> nodeNames) {
        return false;
    }

    public Map<String, IECSNode> getNodes() {
        return null;
    }

    public IECSNode getNodeByKey(String Key) {
        return null;
    }

    public static void main(String[] args) {
        // TODO
    }
}
