package shared.messages;

/**
 * Messages to be sent between ECSClient <-> ECSNode
 */
public class ECSMessage implements KVECSMessage {
	StatusType status;
	int numNodes = -1;
	int cacheSize = -1;
	String replacementStrategy = null;
	int nodeIndex = -1;


	public StatusType getStatus() {
		return status;
	}

	public int getNumNodes() {
		return numNodes;
	}

	public int getCacheSize() {
		return cacheSize;
	}

	public String getReplacementStrategy() {
		return replacementStrategy;
	}

	public int getNodeIndex() {
		return nodeIndex;
	}

	public void setStatus(StatusType _status) {
		this.status = _status;
	}

	public void setNumNodes(int _numNodes) {
		this.numNodes = _numNodes;
	}

	public void setCacheSize(int _cacheSize) {
		this.cacheSize = _cacheSize;
	}

	public void setReplacementStrategy(String _replacementStrategy) {
		this.replacementStrategy = _replacementStrategy;
	}

	public void setNodeIndex(int _nodeIndex) {
		this.nodeIndex = _nodeIndex;
	}
}
