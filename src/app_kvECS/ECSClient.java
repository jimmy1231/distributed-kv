package app_kvECS;

import java.util.Map;
import java.util.Collection;
import java.util.Scanner;

import ecs.IECSNode;

public class ECSClient implements IECSClient {
    private IECSNode ecsNode = null;
    private static final String PROMPT = "ECSClient> ";
    private static final String ADD_NODES = "add_nodes";
    private static final String ADD_NODE = "add_node";
    private static final String QUIT = "quit";
    private static final String START = "start";
    private static final String STOP = "stop";
    private static final String SHUTDOWN = "shutdown";
    private static final String REMOVE_NODE = "remove_node";
    private static final String HELP = "help";

    @Override
    public boolean start() {
        // TODO
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }

    private static void printHelp() {
        System.out.println("HELP!");
    }

    private static boolean assertNumParameters(int expected, int actual) {
        if (expected != actual) {
            System.out.format("ERROR: Expected %d parameters, received %d.\n",
                    expected-1, actual-1);
            return false;
        }
        return true;
    }

    private static boolean handleCommand(String input) {
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
            if (assertNumParameters(1, tokens.length))
                System.out.println("Starting servers...");
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

    public static void main(String[] args) {
        Scanner command = new Scanner(System.in);

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
