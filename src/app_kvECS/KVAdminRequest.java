package app_kvECS;

import com.fasterxml.jackson.databind.ObjectMapper;
import shared.messages.KVMessage;

public class KVAdminRequest implements SocketRequest {

	KVMessage.StatusType status;
	ObjectMapper mapper;

	KVAdminRequest(KVMessage.StatusType _status) {
		this.status = _status;
		mapper = new ObjectMapper();
	}

	public KVMessage.StatusType getStatus() {
		return status;
	}

	public void setStatus(KVMessage.StatusType _status) {
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
		KVAdminRequest request = new KVAdminRequest(KVMessage.StatusType.ERROR);
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
