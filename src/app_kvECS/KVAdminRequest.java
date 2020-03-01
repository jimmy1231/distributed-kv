package app_kvECS;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KVAdminRequest {
	public enum StatusType {
		START,		/* start() req */
		STOP,		/* stop() req */
		SHUTDOWN,	/* shutdown() req */

		SUCCESS,
		ERROR
	}

	StatusType status;
	ObjectMapper mapper;

	KVAdminRequest(StatusType _status) {
		this.status = _status;
		mapper = new ObjectMapper();
	}

	public StatusType getStatus() {
		return status;
	}

	public void setStatus(StatusType _status) {
		this.status = _status;
	}

	public String toString() {
		String str = "";
		try {
			str = mapper.writeValueAsString(this);
			str += "\n";
		} catch(Exception ex) {

		}
		return str;

	}
}
