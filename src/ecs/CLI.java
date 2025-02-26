package ecs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shared.messages.KVDataSet;

import java.util.*;

public class CLI {
	private static Logger logger = LoggerFactory.getLogger(CLI.class);

	private static final String PROMPT = "ECSClient> ";
	private static final String ADD_NODES = "add_nodes";
	private static final String ADD_NODE = "add_node";
	private static final String QUIT = "quit";
	private static final String START = "start";
	private static final String STOP = "stop";
	private static final String SHUTDOWN = "shutdown";
	private static final String REMOVE_NODE = "remove_node";
	private static final String SETUP = "setup";
	private static final String HELP = "help";
	private static final String PRINT_RING = "print_ring";
	private static final String SERVER_DATA = "data";
	private static final String SERVER_REPLICA_DATA = "rdata";
	private static final String REPLICATION = "show_replication";

	private ECSClient client = null;

	/*
	 * Methods used to handle the different commands.
	 * One for each supported command
	 */
	private void handleStart() {
		if (client.start())
			logger.info("SUCCESS: Started all servers.");
		else
			logger.info("ERROR: Could not start the servers.");
	}

	private void handleStop() {
		if (client.stop())
			logger.info("SUCCESS: Stopped all servers.");
		else
			logger.info("ERROR: Could not stop servers.");
	}

	private void handleShutdown() {
		if (client.shutdown())
			logger.info("SUCCESS: Shutdown all servers.");
		else
			logger.info("ERROR: Could not shutdown servers.");
	}

	private void handleAddNode(int cacheSize, String cacheStrategy) {
		IECSNode node = client.addNode(cacheStrategy, cacheSize);
		if (node == null)
			logger.info("ERROR: Could not add node.");
		else {
			// TODO print out new node info
			logger.info("Adding node...");
		}
	}

	private void handleAddNodes(int numNodes, int cacheSize, String cacheStrategy) {
		Collection<IECSNode> nodes = client.addNodes(numNodes, cacheStrategy, cacheSize);
		int count = nodes == null ? 0 : nodes.size();
		if (count == 0)
			logger.info("ERROR: Could not add any nodes.");
		else
			System.out.format("SUCCESS: Added %d nodes\n", count);
	}

	private void handleRemoveNode(String nodeName) {
		Collection<String> names = new ArrayList<String>();
		names.add(nodeName);
		if (client.removeNodes(names))
			System.out.format("SUCCESS: Successfully removed node '%s'\n", nodeName);
		else
			System.out.format("ERROR: Could not remove node '%s'\n", nodeName);
	}

	private void handleSetup(int numNodes, int cacheSize, String cacheStrategy) {
		Collection<IECSNode> nodes = client.setupNodes(numNodes, cacheStrategy, cacheSize);
		int count = nodes == null ? 0 : nodes.size();
		if (count == 0)
			logger.info("ERROR: Setup failed.");
		else
			System.out.format("SUCCESS: Configured %d nodes with Zookeeper\n", count);
	}

	private void handleServerData(String serverName) {
		KVDataSet dataSet = client.getServerData(serverName);
		logger.info("Received data from {}", serverName);
		if (Objects.nonNull(dataSet)) {
			logger.info(dataSet.print(serverName));
		}
	}

	private void handleServerReplicaData(String replicaName, String coordinatorName) {
		KVDataSet dataSet = client.getServerReplicaData(replicaName, coordinatorName);
		logger.info("Received data from {}", replicaName);
		if (Objects.nonNull(dataSet)) {
			logger.info(dataSet.print(String.format(
				"Data from coordinator %s, replicated on: %s",
				coordinatorName, replicaName)));
		}
	}

	private void handleShowReplication(String serverName){
		client.getReplicatedData(serverName);
	}

	/**
	 * Prints out help text
	 */
	private void printHelp() {
		logger.info("HELP!");
	}

	/**
	 * Helper function for handleCommand()
	 * Verifies number of parameters entered by client
	 *
	 * @param expected
	 * @param actual
	 * @return
	 */
	private boolean assertNumParameters(int expected, int actual) {
		if (expected != actual) {
			System.out.format("ERROR: Expected %d parameters, received %d.\n",
					expected-1, actual-1);
			return false;
		}
		return true;
	}

	/**
	 * Parses user input for command and executes desired
	 * ECSClient function
	 *
	 * @param input
	 * @return
	 */
	private boolean handleCommand(String input) {
		String[] tokens = input.split("\\s+");
		String cmd = tokens[0];

		if (input.equals(QUIT)) {
			handleShutdown();
			client.quit();
			return false;
		}
		if (cmd.equals(SETUP)) {
			if (assertNumParameters(4, tokens.length)) {
				int numNodes = Integer.parseInt(tokens[1]);
				int cacheSize = Integer.parseInt(tokens[2]);
				String cacheStrategy = tokens[3];
				handleSetup(numNodes, cacheSize, cacheStrategy);
			}
		}
		else if (cmd.equals(ADD_NODES)) {
			if (assertNumParameters(4, tokens.length)) {
				int numNodes = Integer.parseInt(tokens[1]);
				int cacheSize = Integer.parseInt(tokens[2]);
				String cacheStrategy = tokens[3];
				handleAddNodes(numNodes, cacheSize, cacheStrategy);
			}
		}
		else if (cmd.equals(ADD_NODE)) {
			if (assertNumParameters(3, tokens.length)) {
				int cacheSize = Integer.parseInt(tokens[1]);
				String cacheStrategy = tokens[2];
				handleAddNode(cacheSize, cacheStrategy);
			}
		}
		else if (cmd.equals(START)) {
			if (assertNumParameters(1, tokens.length))
				handleStart();

		}
		else if (cmd.equals(STOP)) {
			if (assertNumParameters(1, tokens.length))
				handleStop();
		}
		else if (cmd.equals(REMOVE_NODE)) {
			if (assertNumParameters(2, tokens.length))
				handleRemoveNode(tokens[1]);

		}
		else if (cmd.equals(SHUTDOWN)) {
			if (assertNumParameters(1, tokens.length))
				handleShutdown();
		}
		else if (cmd.equals(HELP)) {
			if (assertNumParameters(1, tokens.length))
				printHelp();
		}
		else if (cmd.equals(PRINT_RING)) {
			if (assertNumParameters(1, tokens.length))
				client.printRing();
		}
		else if (cmd.equals(SERVER_DATA)) {
			if (assertNumParameters(2, tokens.length))
				handleServerData(tokens[1]);
		}
		else if (cmd.equals(SERVER_REPLICA_DATA)) {
			if (assertNumParameters(3, tokens.length)) {
				handleServerReplicaData(tokens[1], tokens[2]);
			}
		}
		else if (cmd.equals(REPLICATION)) {
			if (assertNumParameters(2, tokens.length))
				handleShowReplication(tokens[1]);
		}
		else {
			logger.info("ERROR: Invalid command!");
			printHelp();

		}
		return true;
	}

	public void run() {
		Scanner command = new Scanner(System.in);
		client = new ECSClient();

		boolean running = true;
		String cmd;
		while(running){
			System.out.print(PROMPT);
			cmd = command.nextLine();
			try {
				running = handleCommand(cmd);
			} catch (Exception e) {
				logger.error("Invalid command!", e);
			}
		}

		logger.info(PROMPT + "Exiting ECSClient! Goodbye.");
		command.close();
	}
}
