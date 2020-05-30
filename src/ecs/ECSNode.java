package ecs;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;

import java.util.Objects;

import static ecs.IECSNode.ECSNodeFlag.*;

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
    @Expose
    private String cacheStrategy;
    @Expose
    private int cacheSize;

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

    public String getCacheStrategy() {
        return cacheStrategy;
    }

    public ECSNode setCacheStrategy(String cacheStrategy) {
        this.cacheStrategy = cacheStrategy;
        return this;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public ECSNode setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    public ECSNodeFlag getEcsNodeFlag() {
        return ecsNodeFlag;
    }

    public void setEcsNodeFlag(ECSNodeFlag ecsNodeFlag) {
        this.ecsNodeFlag = ecsNodeFlag;
    }

    public static ECSNodeFlag getRecoveryTransitionFlag(ECSNodeFlag destinationState) {
        ECSNodeFlag transitionState;
        switch (destinationState) {
            case STOP:
                transitionState = RECOVER_STOP;
                break;
            case START:
                transitionState = RECOVER_START;
                break;
            case IDLE_START:
                transitionState = RECOVER_IDLE_START;
                break;
            default:
                transitionState = destinationState;
        }

        return transitionState;
    }

    public static ECSNodeFlag transitionRecoveryFlag(ECSNodeFlag transitionState) {
        ECSNodeFlag destinationState;
        switch (transitionState) {
            case RECOVER_STOP:
                destinationState = STOP;
                break;
            case RECOVER_START:
                destinationState = START;
                break;
            case RECOVER_IDLE_START:
                destinationState = IDLE_START;
                break;
            default:
                destinationState = transitionState;
        }

        return destinationState;
    }

    /**
     * Determine whether this node was newly created as part
     * of the recovery process. If it is, then return true,
     * else returns false.
     *
     * This method should be used to limit certain operations
     * from being performed with a node who is just recovering
     * (e.g. getReplicas should exclude this node).
     */
    public boolean isRecovering() {
        switch (ecsNodeFlag) {
            case RECOVER_STOP:
            case RECOVER_START:
            case RECOVER_IDLE_START:
                return true;
            default:
        }

        return false;
    }

    public boolean isSameServer(ECSNode node) {
        return uuid.equals(node.uuid);
    }

    public void setNodeHashRange(String[] range) {
        this.range = range;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    @Override
    public boolean compareTo(IECSNode _o) {
        ECSNode o;
        if (_o instanceof ECSNode) {
            o = (ECSNode) _o;
        } else {
            return false;
        }

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
