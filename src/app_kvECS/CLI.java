package app_kvECS;

import java.util.Scanner;

public class CLI {
	private static final String PROMPT = "ECSClient> ";
	private static final String ADD_NODES = "add_nodes";
	private static final String ADD_NODE = "add_node";
	private static final String QUIT = "quit";
	private static final String START = "start";
	private static final String STOP = "stop";
	private static final String SHUTDOWN = "shutdown";
	private static final String REMOVE_NODE = "remove_node";
	private static final String HELP = "help";

	private ECSClient client = null;



	/**
	 * Prints out help text
	 */
	private void printHelp() {
		System.out.println("HELP!");
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
			return false;
		}
		if (cmd.equals(ADD_NODES)) {
			if (assertNumParameters(4, tokens.length))
				System.out.println("Adding nodes..");
		}
		else if (cmd.equals(ADD_NODE)) {
			if (assertNumParameters(3, tokens.length))
				System.out.println("Adding a single node...");
		}
		else if (cmd.equals(START)) {
			if (assertNumParameters(1, tokens.length)) {
				if (client.start())
					System.out.println("SUCCESS: Started the servers.");
				else
					System.out.println("ERROR: Could not start servers");
			}
		}
		else if (cmd.equals(STOP)) {
			if (assertNumParameters(1, tokens.length))
				System.out.println("Stopping servers...");
		}
		else if (cmd.equals(REMOVE_NODE)) {
			if (assertNumParameters(2, tokens.length))
				System.out.println("Removing single node...");
		}
		else if (cmd.equals(SHUTDOWN)) {
			if (assertNumParameters(1, tokens.length))
				System.out.println("Shutting down servers...");
		}
		else if (cmd.equals(HELP)) {
			if (assertNumParameters(1, tokens.length))
				printHelp();
		}
		else {
			System.out.println("ERROR: Invalid command!");
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
			running = handleCommand(cmd);
		}

		System.out.println(PROMPT + "Exiting ECSClient!");
		command.close();
	}
}
