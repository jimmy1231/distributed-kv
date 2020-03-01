package app_kvECS;

public class KVAdminRequest {
	public enum StatusType {
		START,		/* start() req */
		STOP,		/* stop() req */
		SHUTDOWN	/* shutdown() req */
	}

	StatusType status;

	KVAdminRequest(StatusType _status) {
		this.status = _status;
	}

	public StatusType getStatus() {
		return status;
	}

	public void setStatus(StatusType _status) {
		this.status = _status;
	}
}
