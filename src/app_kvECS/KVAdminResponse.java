package app_kvECS;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KVAdminResponse {
	public enum StatusType {
		START,		/* start() req */
		STOP,		/* stop() req */
		SHUTDOWN,	/* shutdown() req */

		SUCCESS,
		ERROR
	}

	KVAdminResponse.StatusType status;
	ObjectMapper mapper;

	KVAdminResponse() {
		this.status = null;
		mapper = new ObjectMapper();
		mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

	}

	KVAdminResponse(KVAdminResponse.StatusType _status) {
		this.status = _status;
		mapper = new ObjectMapper();
		mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
	}

	public KVAdminResponse.StatusType getStatus() {
		return status;
	}

	public void setStatus(KVAdminResponse.StatusType _status) {
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
