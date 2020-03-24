package app_kvECS;

import ecs.ECSNode;
import ecs.IECSNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HeartbeatMonitor extends Thread {
	private static final Logger logger = LoggerFactory.getLogger(HeartbeatMonitor.class);
	private static final List<IECSNode.ECSNodeFlag> WHITELIST_FLAGS = Arrays.asList(
		IECSNode.ECSNodeFlag.START,
		IECSNode.ECSNodeFlag.STOP
	);
	public static final int TIMEOUT_MS = 5000;
	private static final String LOG_PREFIX = "[HEARTBEAT_MONITOR]";

	private ECSClient thisClient;
	private boolean isRunning;
	private long restPeriodMS;

	public HeartbeatMonitor(ECSClient client, long restPeriodMS) {
		logger.debug("{}: Initialized. Rest Period={}", LOG_PREFIX, restPeriodMS);
		thisClient = client;
		isRunning = true;
		this.restPeriodMS = restPeriodMS;
	}

	@Override
	public void run() {
		/*
		 * (1) While awake, send requests to servers qualifying under
		 * 	   WHITELIST_FLAGS
		 * (2) Block response until either: a) response received, b)
		 * 	   timeout reached -> consider server as failed
		 * (3) For all failed servers, call client.recoverServers()
		 */
		Iterator<Map.Entry<String, IECSNode>> it;
		Map.Entry<String, IECSNode> entry;
		List<ECSNode> failed;
		while (isRunning) {
			logger.debug("{}: Awake, check server status..", LOG_PREFIX);
			failed = new ArrayList<>();

			// (1)
			it = thisClient.getNodes().entrySet().iterator();
			ECSNode server;
			while (it.hasNext()) {
				entry = it.next();
				server = (ECSNode) entry.getValue();
				logger.debug("{}: Server={}", LOG_PREFIX, server.getUuid());

				if (WHITELIST_FLAGS.contains(server.getEcsNodeFlag())) {
					logger.debug("{}: Pinging server {}", LOG_PREFIX,
						server.getUuid());

					// (2)
					if (!pingServerSync(server)) {
						logger.debug("{}: Server failure detected: {} | STATUS={}",
							LOG_PREFIX,
							server.getUuid(), server.getEcsNodeFlag());
						failed.add(server);
					}
				}
			}

			// (3)
			thisClient.recoverServers(failed);

			if (!sleepFor(restPeriodMS)) {
				logger.debug("{}: Thread interrupted, exiting", LOG_PREFIX);
				isRunning = false;
			}
		}
	}

	public void setIsRunning(boolean isRunning) {
		this.isRunning = isRunning;
	}

	private boolean pingServerSync(ECSNode server) {
		try {
			ECSRequestsLib.heartbeat(server, TIMEOUT_MS);
		} catch (Exception e) {
			return false;
		}

		logger.debug("{}: Server {} is alive!", LOG_PREFIX,
			server.getUuid());
		return true;
	}

	private boolean sleepFor(long ms) {
		try {
			Thread.sleep(ms);
		} catch (Exception e) {
			return false;
		}

		return true;
	}
}
