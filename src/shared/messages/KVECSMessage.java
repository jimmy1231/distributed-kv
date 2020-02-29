package shared.messages;

public interface KVECSMessage {
	public enum StatusType {
		ADD_NODES,		/* addNodes() - <num_nodes> <cache_size> <replacement_strat>*/
		ADD_NODE,		/* addNode() - <cache_size> <replacement_strat> */
		START,			/* start() */
		STOP,			/* stop() */
		SHUTDOWN,		/* shutdown() */
		REMOVE_NODE,	/* removeNode() - <node_index>*/

		ACK,		/* ECSNode acknowledged the request */
		SUCCESS,	/* ECSClient request was successful */
		ERROR		/* ECSClient request failed*/
	}

	/**
	 * @return status enum to identify request types as specified
	 * in StatusType enum
	 */
	public StatusType getStatus();

	/**
	 * @return num_nodes parameter for ADD_NODES request
	 */
	public int getNumNodes();

	/**
	 * @return cache_size parameter for ADD_NODE(S) requests
	 */
	public int getCacheSize();

	/**
	 * @return replacement_strat parameter for ADD_NODE(S) requests
	 */
	public String getReplacementStrategy();

	/**
	 *
	 * @return node_index parameter for REMOVE_NODE request
	 */
	public int getNodeIndex();

	/* Setters */
	public void setStatus(StatusType _status);
	public void setNumNodes(int _numNodes);
	public void setCacheSize(int _cacheSize);
	public void setReplacementStrategy(String _replacementStrategy);
	public void setNodeIndex(int _nodeIndex);

}
