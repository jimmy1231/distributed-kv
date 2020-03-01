package app_kvECS;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KVAdminResponse implements SocketResponse {
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

	@Override
	public String toJsonString() {
		String json;
		try {
			json = mapper.writeValueAsString(this);
		} catch (Exception e) {
			System.err.printf("ERROR SERIALIZING USING JACKSON: %s",
				e.getMessage());
			return "{\"message\": \"could not serialize\"}";
		}

		return json;
	}
	@Override
	public SocketResponse fromJsonString(String json) {
		String str = "";
		try {
			str = mapper.writeValueAsString(this);
			str += "\n";
		} catch(Exception ex) {

		}

		return this;
	}
}
