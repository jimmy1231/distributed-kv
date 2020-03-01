package app_kvECS;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KVAdminRequest implements SocketRequest {
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

	@Override
	public String toJsonString() {
		String str = "";
		try {
			str = mapper.writeValueAsString(this);
			str += "\n";
		} catch(Exception ex) {

		}
		return str;
	}

	@Override
	public SocketRequest fromJsonString(String json) {
		KVAdminRequest request = new KVAdminRequest(StatusType.ERROR);
		try {
			request = mapper.readValue(json, KVAdminRequest.class);
		} catch (Exception e) {
			System.err.printf("ERROR JACKSON DESERIALIZATION: %s",
				e.getMessage());
		}

		this.status = request.status;
		return this;
	}
}
