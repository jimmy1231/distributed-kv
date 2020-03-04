package shared.messages;

import app_kvECS.KVServerMetadata;
import jdk.net.SocketFlow;

public interface KVMessage {
	
	public enum StatusType {
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, 	/* Delete - request successful */
		CLIENT_ERROR, 	/* General client error */

		SERVER_STOPPED, 		/* Server is stopped, no requests are processed */
		SERVER_STARTED, 		/* Server is started, processing all client & ECS requests */
		SERVER_WRITE_LOCK, 		/* Server locked for out, only get possible. if server sees this, call lockWrite() */
		SERVER_NOT_RESPONSIBLE, /* Requset not successful, server not responsible for key */
		SERVER_WRITE_UNLOCK, /* if server sees this, call unLockWrite() */
		SERVER_INIT, /* if server sees this. call initKVServer() */
		SERVER_MOVEDATA, /* if server sees this, call moveData() */
		SERVER_UPDATE, /* update metadata */
		SERVER_TRANSFER,
		SERVER_DUMP_DATA, /* Server should dump data and return it to ECS */
		
		START,				/* ECSClient sending a START request to Server */
		STOP,				/* ECS sends STOP request to server */
		SHUTDOWN,			/* ECS sends SHUTDOWN request to server */

		SUCCESS,	/* ECS request was a success */
		ERROR		/* ECS request failed */
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();

	public void setStatus(StatusType newStatus);

	public void setValue(String newValue);

	public KVServerMetadata getMetadata();

	public void setMetadata(KVServerMetadata newMetadata);
}


