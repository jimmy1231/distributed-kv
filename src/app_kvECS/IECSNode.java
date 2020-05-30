package app_kvECS;

public interface IECSNode {
    public enum ECSNodeFlag {
        STOP,               /* Node has stopped */
        START,              /* Node has started */
        STATE_CHANGE,       /* Node state has changed */
        KV_TRANSFER,        /* Data transfer occurred, aka. WRITE_LOCK */
        SHUT_DOWN,          /* Node has shut down */
        UPDATE,             /* Node has updated */
        TRANSFER_FINISH,    /* Data transfer operation finished */
        IDLE,               /* Initial state of ECSNode */

        /* Transition states */
        IDLE_START,         /* In transition from IDLE to START */
        START_STOP,         /* In transition from START to STOP */
        START_SHUT_DOWN,    /* In transition from START to SHUT_DOWN */
        RECOVER_STOP,       /* In transition recovered to STOP -> use in recovery only */
        RECOVER_START,      /* In transition recovered to START -> use in recovery only */
        RECOVER_IDLE_START  /* In transition recovered to IDLE_START -> use in recovery only */
    }

    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName();

    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost();

    /**
     * @return  the port number of the node (ie 8080)
     */
    public int getNodePort();

    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public String[] getNodeHashRange();

    public boolean compareTo(IECSNode _o);
}
